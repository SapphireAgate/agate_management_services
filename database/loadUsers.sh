CREATE DATABASE IF NOT EXISTS UserManagementService;
USE UserManagementService;

DROP TABLE IF EXISTS Users,Groups,UserGroups,ReadingPolicy;

CREATE TABLE IF NOT EXISTS Users ( userID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, username VARCHAR(20), password VARCHAR(20));
CREATE TABLE IF NOT EXISTS Groups (groupID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, groupName VARCHAR(20), owner INT);
CREATE TABLE IF NOT EXISTS UserGroups (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, groupID INT, userID INT);
CREATE TABLE IF NOT EXISTS ReadingPolicy (PolicyID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, userID INT, groupID INT);

INSERT INTO Users (username,password) VALUES
    	('adriana','12345678'),
	('elisabeth','87654321'),
	('john','smith');


SELECT * from Credentials;


INSERT INTO Groups (groupName, owner) VALUES
	('friends',1),
	('collegues',1),
	('family',1),
	('friends',2),
	('collegues',2),
	('family',2),
	('friends',3),
	('collegues',3),
	('family',3);

SELECT * from Groups;



INSERT INTO UserGroups (groupID, userID) VALUES
	(1,2),
	(2,3),
	(6,1),
	(9,2);

SELECT * from UserGroups;


INSERT INTO ReadingPolicy (userID, groupID) VALUES
	(1,2),
	(1,3),
	(2,1),
	(3,2);

SELECT * FROM ReadingPolicy;








