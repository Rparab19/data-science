import string
import datetime as dt
print('#1 Removing leading or lagging spaces from a data entry'); baddata = " Data Science with too many spaces is bad!!! " print('>',baddata,'<')
cleandata=baddata.strip() print('>',cleandata,'<')
print('#2 Removing nonprintable characters from a data entry') printable = set(string.printable)
baddata = "Data\x00Science with\x02 funny characters is \x10bad!!!" cleandata=''.join(filter(lambda x: x in string.printable,baddata)) print('Bad Data : ',baddata);
print('Clean Data : ',cleandata) baddate = dt.date(2024, 8, 14)
baddata=format(baddate,'%Y-%m-%d')
gooddate = dt.datetime.strptime(baddata,'%Y-%m-%d') gooddata=format(gooddate,'%d %B %Y')
print('Bad Data : ',baddata) print('Good Data : ',gooddata)
