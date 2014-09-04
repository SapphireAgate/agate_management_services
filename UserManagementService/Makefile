FLAGS=$(shell mysql_config --cflags)
LIBS=$(shell mysql_config --libs)

SERVER_SRC_FILES=UserManagementService.cpp AgateUtil.cpp

all:
	gcc -o ums $(FLAGS) $(SERVER_SRC_FILES) -lpthread $(LIBS)

clean:
	rm ums
