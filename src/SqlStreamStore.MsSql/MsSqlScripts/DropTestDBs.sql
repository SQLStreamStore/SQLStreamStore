/* When tests fail, sometimes there are databases left behind. Run this script to clean up */
use master
go
declare @dbnames nvarchar(max)
declare @statement nvarchar(max)
set @dbnames = ''
set @statement = ''
select @dbnames = @dbnames + ',[' + name + ']' from sys.databases where name like 'StreamStoreTests%'
if len(@dbnames) = 0
    begin
    print 'no databases to drop'
    end
else
    begin
    set @statement = 'drop database ' + substring(@dbnames, 2, len(@dbnames))
    print @statement
    exec sp_executesql @statement
    end