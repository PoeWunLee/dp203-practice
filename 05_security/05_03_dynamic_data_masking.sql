-- Lab - Azure Synapse - Data Masking
--Authenticate
CREATE LOGIN newusr
	WITH PASSWORD = 'Azure123' 
GO

--Authorization
CREATE USER newusr
	FOR LOGIN newusr
	WITH DEFAULT_SCHEMA = dbo

EXEC sp_addrolemember 'db_datareader', 'newusr';  


--Add the data masking from UI. generally privileged users (e.g. SQL admin) is able to see unmasked data

--To grant user unmask privileges, following command
GRANT UNMASK TO MaskingTestUser;

EXECUTE AS USER = 'MaskingTestUser';

SELECT * FROM Data.Membership;

REVERT;
  
-- Removing the UNMASK permission
REVOKE UNMASK TO MaskingTestUser;