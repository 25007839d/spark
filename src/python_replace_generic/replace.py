import configparser
config=configparser.ConfigParser()
config.read(r"C:\Users\Dell\PycharmProjects\spark\python_replace_generic\old.ini")

st = config.get('value','string')
old_value = config.get('value','old')
new_value = config.get('value','new')
s=''
for i in st.split():
    if i==old_value:
        s=s+' '+new_value
    else:
        s=s+" "+i

print(s)






