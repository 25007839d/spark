
'''
class myclass(object):
    def show (self):
        print("i am a method")
x= myclass()
x.show()
'''
# example 2
'''
class dushyant():
    def __init__(self,x):
        self.model=x

    def mob(self,p):
        self.price=p
        print (self.model,self.price)

realme=dushyant('c-6')
realme.mob(9850)
print(id(realme))

moto=dushyant('c+')
moto.mob(6000)
print(id(moto))

list=[]
for i in range(3):
    name=input('enter your string')
    list.append(name)
    print(list)

f= open('stu.text',mode='w')
f.write('hello world \n')
f.write('greeky shoes \n')
f.write('how are you')
f.close()


f= open('stu1.text',mode='x')
f.write('hello world \n')
f.write('greeky shoes \n')
f.write('how are you')
f.close()



f= open('stu.text',mode='a')
f.write('hello world')
f.write('greeky')
f.close()
'''
f= open('stu.text',mode='r')
f.close()
print(f.closed)
print(f.readable())




