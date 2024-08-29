-- Lab - Azure Synapse - Column-Level Security

-- First lets create a table

CREATE TABLE [dbo].[CourseOrders] 
(  
    OrderID int,  
    Agent varchar(50),  
    Course varchar(50),  
    Quantity int  
);  

-- Insert rows into the table

INSERT INTO [dbo].[CourseOrders] VALUES(1,'AgentA','AZ-900',5);
INSERT INTO [dbo].[CourseOrders] VALUES(2,'AgentA','DP-203',4);
INSERT INTO [dbo].[CourseOrders] VALUES(3,'AgentB','AZ-104',5);
INSERT INTO [dbo].[CourseOrders] VALUES(4,'AgentB','AZ-204',6);
INSERT INTO [dbo].[CourseOrders] VALUES(5,'AgentA','AZ-305',7);
INSERT INTO [dbo].[CourseOrders] VALUES(6,'AgentB','DP-900',8);
-- Create the database users

CREATE USER Supervisor WITHOUT LOGIN;  
CREATE USER UserA WITHOUT LOGIN;  

-- Grant access to the tables for the users

GRANT SELECT ON [dbo].[CourseOrders] TO Supervisor; 
GRANT SELECT ON [dbo].[CourseOrders](OrderID,Course,Quantity) TO UserA; 


-- Test is for the different users
--User A will be met with error as they are not granted access on Agent column
EXECUTE AS USER = 'UserA';  
SELECT * FROM [dbo].[CourseOrders];
SELECT OrderID,Course,Quantity FROM [dbo].[CourseOrders];
REVERT;  
  
--Supervisor able to select * and not be met with error 
EXECUTE AS USER = 'Supervisor';  
SELECT * FROM [dbo].[CourseOrders];
REVERT; 

-- Drop all of the artefacts

DROP USER Supervisor;
DROP USER UserA;

DROP TABLE [dbo].[CourseOrders];




