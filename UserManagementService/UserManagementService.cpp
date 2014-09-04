#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <mysql.h>
#include <assert.h>

#include "AgateUtil.h"

// sudo apt-get install libmysqlclient-dev

// hard-coded info about the UMS
static const char* HOST = "howe.cs.washington.edu";
static int PORT = 24068;

typedef struct
{
    int sockfd;
    struct sockaddr address;
    socklen_t addr_len;
} connection_t;


static int _read_command_len(int sockfd) {
    char* buf = _agate_util_read_x_bytes_from_socket(sockfd, sizeof(int));
    int r = _agate_util_int_from_byte_array(buf);
    free(buf);
    return r;
}


void _finish_with_error(MYSQL *con, bool rollback)
{
    fprintf(stderr, "%s\n", mysql_error(con));
    if (rollback)
        mysql_query(con, "ROLLBACK;");
    mysql_close(con);
    exit(1);        
}

/*
 *  Commands
 */

static bool login(int sockfd, char* command) {
    // TODO: not thread safe
    char* username = strtok(command, " ");
    char* password = strtok(NULL, " ");

    assert(username != NULL);
    assert(password != NULL);

    /* Initialize DB structure */
    MYSQL* db_con = mysql_init(NULL);
    if (db_con == NULL) {
        _finish_with_error(db_con, false);
    }

    /* Open DB connection */
    if (mysql_real_connect(db_con, "localhost", "root", "root", 
          "UserManagementService", 0, NULL, 0) == NULL) {
        _finish_with_error(db_con, false);
    }

    /* Compose the query */
    char* query = (char*)malloc(strlen(username) + strlen(password) + 50);
    sprintf(query, "SELECT userId FROM Users where username=\'%s\' and password=\'%s\'",
                    username, password);

    if (mysql_query(db_con, query)) {
        _finish_with_error(db_con, false);
    }

    MYSQL_RES *result = mysql_store_result(db_con);

    int out_len = 2 * sizeof(int);
    char* out = (char*)malloc(out_len);
    char* tmp =_agate_util_add_int(out, sizeof(int));

    if (result == NULL) {
        _agate_util_add_int(tmp, -1);
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, false);
    } else {
        MYSQL_ROW row = mysql_fetch_row(result);
        if (row != NULL) {
            _agate_util_add_int(tmp, atoi(row[0]));
            mysql_free_result(result);
        } else {
            _agate_util_add_int(tmp, -1);
        }
    }

    _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
    mysql_close(db_con);
    free(query);
    return 1;
}

static bool add_user(int sockfd, char* command) {
    // TODO: strtok not thread safe 
    char* username = strtok(command, " ");
    char* password = strtok(NULL, " ");

    assert(username != NULL);
    assert(password != NULL);

    /* Initialize result */
    int out_len = 2 * sizeof(int);
    char* out = (char*)malloc(out_len);
    char* tmp =_agate_util_add_int(out, sizeof(int));
    _agate_util_add_int(tmp, 0);

    /* Initialize DB structure */
    MYSQL* db_con = mysql_init(NULL);
    if (db_con == NULL) {
        _finish_with_error(db_con, false);
    }

    /* Open DB connection */
    if (mysql_real_connect(db_con, "localhost", "root", "root", 
          "UserManagementService", 0, NULL, 0) == NULL) {
        _finish_with_error(db_con, false);
    }

    /* Start transaction */
    if (mysql_query(db_con, "START TRANSACTION;")) {
        _finish_with_error(db_con, false);
    }

    /* Check if username not taken */
    char* query = (char*)malloc(strlen(username) + strlen(password) + 50);

    sprintf(query, "SELECT userId FROM Users WHERE username=\'%s\';", username);
    if (mysql_query(db_con, query)) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    MYSQL_RES *result = mysql_store_result(db_con);
    if (result == NULL) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    if (row != NULL) {
        mysql_query(db_con, "ROLLBACK;");
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        mysql_close(db_con);
        mysql_free_result(result);
        return 0;
    }
    mysql_free_result(result);

    /* User not taken, insert */
    sprintf(query, "INSERT INTO Users(username, password) values(\'%s\', \'%s\');", username, password);
    if (mysql_query(db_con, query)) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    /* End transaction */
    if (mysql_query(db_con, "COMMIT;")) {
        _finish_with_error(db_con, true);
    }

    _agate_util_add_int(tmp, 1);
    _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
    mysql_close(db_con);
    free(query);
    return 1;
}

static bool add_group(int sockfd, char* command) {
    int owner;
    char* groupname = _agate_util_get_int(command, &owner);

    assert(owner > 0);
    assert(groupname != NULL);

    /* Initialize result */
    int out_len = 2 * sizeof(int);
    char* out = (char*)malloc(out_len);
    char* tmp =_agate_util_add_int(out, sizeof(int));
    _agate_util_add_int(tmp, 0);

    /* Initialize DB structure */
    MYSQL* db_con = mysql_init(NULL);
    if (db_con == NULL) {
        _finish_with_error(db_con, false);
    }

    /* Open DB connection */
    if (mysql_real_connect(db_con, "localhost", "root", "root", 
          "UserManagementService", 0, NULL, 0) == NULL) {
        _finish_with_error(db_con, false);
    }

    /* Start transaction */
    if (mysql_query(db_con, "START TRANSACTION;")) {
        _finish_with_error(db_con, false);
    }

    /* Check if groupname for owner not taken */
    char* query = (char*)malloc(strlen(groupname) + 100);
    sprintf(query, "SELECT g.groupName FROM Groups g WHERE g.groupName=\'%s\' and g.owner = %d;", groupname, owner);

    if (mysql_query(db_con, query)) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    MYSQL_RES *result = mysql_store_result(db_con);
    if (result == NULL) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    if (row != NULL) {
        mysql_query(db_con, "ROLLBACK;");
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        mysql_close(db_con);
        mysql_free_result(result);
        free(query);
        return 0;
    }
    mysql_free_result(result);

    /* Group not taken, insert */
    sprintf(query, "INSERT INTO Groups(groupName, owner) values(\'%s\', %d);", groupname, owner);
    if (mysql_query(db_con, query)) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    /* End transaction */
    if (mysql_query(db_con, "COMMIT;")) {
        _finish_with_error(db_con, true);
    }

    _agate_util_add_int(tmp, 1);
    _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
    mysql_close(db_con);
    free(query);
    return 1;
}

static bool add_user_to_group(int sockfd, char* command) {
    int owner;
    command = _agate_util_get_int(command, &owner);

    char* groupname = strtok(command, " ");
    char* username = strtok(NULL, " ");

    assert(owner > 0);
    assert(groupname != NULL);
    assert(username != NULL);

    /* Initialize result */
    int out_len = 2 * sizeof(int);
    char* out = (char*)malloc(out_len);
    char* tmp =_agate_util_add_int(out, sizeof(int));
    _agate_util_add_int(tmp, 0);

    /* Initialize DB structure */
    MYSQL* db_con = mysql_init(NULL);
    if (db_con == NULL) {
        _finish_with_error(db_con, false);
    }

    /* Open DB connection */
    if (mysql_real_connect(db_con, "localhost", "root", "root", 
          "UserManagementService", 0, NULL, 0) == NULL) {
        _finish_with_error(db_con, false);
    }

    /* Start transaction */
    if (mysql_query(db_con, "START TRANSACTION;")) {
        _finish_with_error(db_con, false);
    }

    /* Check if owner has group groupname */
    char* query = (char*)malloc(strlen(groupname) + strlen(username) + 100);
    sprintf(query, "SELECT g.owner FROM Groups g WHERE g.groupName=\'%s\';", groupname);

    if (mysql_query(db_con, query)) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }
    
    MYSQL_RES *result = mysql_store_result(db_con);
    if (result == NULL) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == NULL) { // owner doesn't have group groupname
        mysql_query(db_con, "ROLLBACK;");
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        mysql_close(db_con);
        mysql_free_result(result);
        free(query);
        return 0;
    }
    mysql_free_result(result);

    /* Owner has group, get group id */
    sprintf(query, "SELECT g.groupID FROM Groups g WHERE g.groupName=\'%s\' and owner=%d;", groupname, owner);

    if (mysql_query(db_con, query)) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    int groupID = -1;    
    result = mysql_store_result(db_con);
    if (result == NULL) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    row = mysql_fetch_row(result);
    if (row == NULL) { // can't get group id
        mysql_query(db_con, "ROLLBACK;");
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        mysql_close(db_con);
        mysql_free_result(result);
        free(query);
        return 0;
    } else {
        assert(row[0] != NULL);
        groupID = atoi(row[0]);
    }
    mysql_free_result(result);

    /* Get user id */
    sprintf(query, "SELECT u.userID FROM Users u WHERE u.username=\'%s\';", username);

    if (mysql_query(db_con, query)) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    int userID = -1;
    result = mysql_store_result(db_con);
    if (result == NULL) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    row = mysql_fetch_row(result);
    if (row == NULL) { // can't get group id
        mysql_query(db_con, "ROLLBACK;");
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        mysql_close(db_con);
        mysql_free_result(result);
        free(query);
        return 0;
    } else {
        assert(row[0] != NULL);
        userID = atoi(row[0]);
    }
    mysql_free_result(result);

    /* Verify user not already in group */
    sprintf(query, "SELECT * FROM UserGroups ug WHERE ug.groupID=%d and ug.userID=%d;", groupID, userID);

    if (mysql_query(db_con, query)) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    result = mysql_store_result(db_con);
    if (result == NULL) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    row = mysql_fetch_row(result);
    if (row != NULL) { // already in the group
        mysql_query(db_con, "ROLLBACK;");
        _agate_util_add_int(tmp, 1); // return OK
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        mysql_close(db_con);
        mysql_free_result(result);
        free(query);
        return 0;
    }
    mysql_free_result(result);
 
    /* Add user to group */
    sprintf(query, "INSERT INTO UserGroups(groupID, userID) values(%d, %d);", groupID, userID);
    if (mysql_query(db_con, query)) {
        _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
        _finish_with_error(db_con, true);
    }

    /* End transaction */
    if (mysql_query(db_con, "COMMIT;")) {
        _finish_with_error(db_con, true);
    }

    _agate_util_add_int(tmp, 1);
    _agate_util_write_x_bytes_to_socket(sockfd, out, out_len);
    mysql_close(db_con);
    free(query);
    return 1;
}

static bool can_flow(int sockfd, char* command) {
}

void* process_connection(void* ptr) {
    char command_type;
    connection_t* conn;

    if (!ptr) pthread_exit(0); 
    conn = (connection_t *)ptr;

    /* Read command type */
    read(conn->sockfd, &command_type, 1);
    
    /* Get command message */
    char* command = _agate_util_read_x_bytes_from_socket(conn->sockfd,
                                               _read_command_len(conn->sockfd));

    switch (command_type) {
       case '1':
           printf("Processing login request.\n");
           login(conn->sockfd, command);
           break;
       case '2':
           printf("Processing add_user request.\n");
           add_user(conn->sockfd, command);
           break;
       case '3':
           printf("Processing add_group request.\n");
           add_group(conn->sockfd, command);
           break;
       case '4':
           printf("Processing add_user_to_group request.\n");
           add_user_to_group(conn->sockfd, command);
           break;
       case '5':
           printf("Processing can_flow request.\n");
           can_flow(conn->sockfd, command);
           break;
       default :
           fprintf(stderr, "UMS: error: command not supported.\n");
    }

    /* Close socket and clean up */
    close(conn->sockfd);
    free(conn);
    pthread_exit(0);
}

int main(int argc, char ** argv)
{
    int sockfd = -1;
    struct sockaddr_in address;
    connection_t* connection;
    pthread_t thread;

    /* Create socket */
    sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sockfd <= 0) {
        fprintf(stderr, "UMS: error: cannot create socket.\n");
        return 1;
    }

    /* Bind socket to port */
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);
    if (bind(sockfd, (struct sockaddr *)&address, sizeof(struct sockaddr_in)) < 0) {
        fprintf(stderr, "UMS: error: cannot bind socket to port %d.\n", PORT);
        return 1;
    }

    /* Listen on port */
    if (listen(sockfd, 5) < 0) {
        fprintf(stderr, "UMS: error: cannot listen on port.\n");
        return 1;
    }

    printf("UMS: Ready and listening...\n");

    while (1) {
        /* Accept incoming connections */
        connection = (connection_t *)malloc(sizeof(connection_t));
        connection->sockfd = accept(sockfd, &connection->address, &connection->addr_len);
        if (connection->sockfd <= 0) {
            free(connection);
        } else {
            /* Start a new thread but do not wait for it */
                pthread_create(&thread, 0, process_connection, (void *)connection);
                pthread_detach(thread);
        }
    }	
    return 0;
}

