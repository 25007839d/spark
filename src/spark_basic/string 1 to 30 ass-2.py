#1.	Python program to check if a string is palindrome or not
'''
x=int(input('ente the palindrome number'))
a=0
b=0

for i in range (x):
  if x > 0:
   a=x%10
   b=b*10+a
   x=x//10
print(b)
'''
#2.	Python program to check whether the string is Symmetrical or Palindrome
'''
x=int(input('ente the palindrome number'))
a=0
b=0
c=x
for i in range (x):

  if x > 0:
   a=x%10
   b=b*10+a
   x=x//10
print(b)

if b==c:
    print('value is palindrome')
else:
    print('value not ispalindrome')
'''
#3.	Reverse words in a given String in Python
'''
def isPalindrome(s):
	return s == s[::-1]

s = "alayalam"
ans = isPalindrome(s)

if ans:
	print("Yes")
else:
	print("No")
'''
#4.	Ways to remove i’th character from string in Python
'''
test_str = "GeeksForGeeks"
print ("The original string is : " + test_str)
new_str = ""
for i in range(len(test_str)):
	if i != 3:
		new_str = new_str + test_str[i]
'''

#5.	Python | Check if a Substring is Present in a Given String
'''
def check(string, sub_str):
    if (string.find(sub_str) == -1):
        print("NO")
    else:
        print("YES")
string = "geeks for geeks"
sub_str = "geek"
check(string, sub_str)
'''
#6.	Python – Words Frequency in String Shorthands
# Python3 code to demonstrate working of
# Words Frequency in String Shorthands
'''
str = ' Geeks are good and Geeks like Gfg'

print("The original string is : ", str)

# Using dictionary comprehension + count() + split()
res = {key: str.count(key) for key in str.split()}

print("The words frequency : ",res)
'''
#7.	Python – Convert Snake case to Pascal case
'''
string = "geeks for geeks"
print('string befor convert:',string)
s=string.replace("_"," ").title().replace(" "," ")
print("The string after changing case:",s)
'''
#8.	Find length of a string in python (4 ways)
'''
string = "geeks for geeks"
str = (len(string))
print(str)
'''
#9.	Python program to print even length words in a string
'''
string = "geeks"
n=(len('greeks'))
print(n)
'''
#0.	Python program to accept the strings which contains all vowels
'''
def check(string) :

	string = string.lower()

	vowels = set("aeiou")
	s = set({})
	for char in string:
		if char in vowels :
			s.add(char)
		else:
			pass
	if len(s) == len(vowels) :
		print("Accepted")
	else :
		print("Not Accepted")
if __name__ == "__main__" :

	string = "SEEquoiaL"
	check(string)
'''

#11.	Python | Count the Number of matching characters in a pair of string
'''
import re
str1 = "mohan is a goog boy"
str2 = "sohan is a good boy"
c= 0
for i in str1:
    if re.search(i,str2):
        c=c+1
print('num of matching charcters are:',c)
'''
#12.	Remove all duplicates from a given string in Python
'''
def fix(string):
    s = set()
    list = []
    for ch in string:
        if ch not in s:
            s.add(ch)
            list.append(ch)
            return''. join(list)
        string = "greegreeks"
        print(fix(string))
#or
s="greeksfor greeks"
s=''.join(set(s))
print(s)
#13.	Python – Least Frequent Character in String
str = "GeeksforGeeks"
print ("The original string is : ", str)

freq = {}
for i in str:
	if i in freq:
		freq[i] += 1
	else:
		freq[i] = 1
res = min(freq, key =freq.get)

print ("The min of all characters in GeeksforGeeks is : ", res)
'''
#14.	Python | Maximum frequency character in String
'''
str = "GeeksforGeeks"
print ("The original string is : ", str)

freq = {}
for i in str:
	if i in freq:
		freq[i] += 1
	else:
		freq[i] = 1
res = max(freq, key =freq.get)

print ("The max of all characters in GeeksforGeeks is : ", res)
'''

#15.	Python | Program to check if a string contains any special charact
'''
import re
string=input('enter any string:')
special_char =re.compile('[@_%$#$&^&()]')
if (special_char.search(string) == None):
	print("string does not contain any sp char.")
else:
	print("string contains  sp char.")
'''
#16.	Generating random strings until a given string is generated





#17.	Find words which are greater than given length k.

'''
def word_k(k,s):
	word = s.split(" ")
	for x in word:
		if len(x)>k:
			print(x)
k = 3
s = " mohan is are ra  have was m a good boy"
word_k(k,s)
'''

#18.	Python program for removing i-th character from a string
'''
s = " !mmohan"
new_str=s[1:]
print(new_str)
'''


#19.	Python program to split and join a string
'''
def split_string(string):
	list_string=string.split(' ')
	return list_string
def join_string(list_string):
	string = "_".join (list_string)
	return string
if __name__ == '__main__':
    string = "mohan is a good boy"
    list_string = split_string(string)
    print(list_string)
    new_string = join_string(list_string)
    print (new_string)
'''
#20.	Python | Check if a given string is binary string or not
'''
def check(string):
	b=set(string)
	s= {'0','1'}
	if s == b or b == {'o'} or b =={'1'}:
	 print('binary string')
	else:
		print('non binary string')

s1="0011010101"
check(s1)
s2 ="1010100200111"
check(s2)
#21.	Python program to find uncommon words from two Strings
def uncomman_words(s1,s2):
	count = {}
	for word in s1.split():
		count[word] =count.get(word,0)+1
	for word in s2.split():
		count[word]= count[word] =count.get(word,0)+1
	return [word for word in count if count [word]==1]

s1	="studytonight"
s2 ="welcome to studytonight "

print('uncommon_words(s1,s2)')
'''
#22.	Python – Replace duplicate Occurrence in String
'''
def UncommonWords(A, B):
	count = {}
	for word in A.split():
		count[word] = count.get(word, 0) + 1
	for word in B.split():
		count[word] = count.get(word, 0) + 1
	return [word for word in count if count[word] == 1]

A = "Geeks for Geeks"
B = "Learning from Geeks for Geeks"

print(UncommonWords(A, B))
'''


#23.	Python – Replace multiple words with K
'''

test_str = 'Gfg is best . Gfg also has Classes now. \
				Classes help understand better . '

print("The original string is : " + str(test_str))

repl_dict = {'Gfg' : 'It', 'Classes' : 'They' }
test_list = test_str.split(' ')
res = set()
for idx, ele in enumerate(test_list):
	if ele in repl_dict:
		if ele in res:
			test_list[idx] = repl_dict[ele]
		else:
			res.add(ele)
res = ' '.join(test_list)

print("The string after replacing : " + str(res))'''
#24.	Python | Permutation of a given string using inbuilt functio


'''
def allPermutations(str):

	# Get all permutations of string 'ABC'
	permList = permutations(str)
	for perm in list(permList):
		print(''.join(perm))


# Driver program
if __name__ == "__main__":
	str = 'ABC'
	allPermutations(str)
'''

#25.	Python | Check for URL in a String
# Python code to find the URL from an input string
'''
import re
def Find(string):
	regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
	url = re.findall(regex, string)
	return [x[0] for x in url]
string = 'My Profile: https://auth.get up early in the morning.org/user/Chinmoy%20Lenka/articles in the portal of https://www.geeksforgeeks.org/'
print("Urls: ", Find(string))
'''

#26.	Execute a String #of Code in Python
#27.	String slicing in Python to rotate a string

# Function to rotate string left and right by d length
'''
def rotate(input,d):

	# slice string in two parts for left and right
	Lfirst = input[0 : d]
	Lsecond = input[d :]
	Rfirst = input[0 : len(input)-d]
	Rsecond = input[len(input)-d : ]

	# now concatenate two parts together
	print ("Left Rotation : ", (Lsecond + Lfirst) )
	print ("Right Rotation : ", (Rsecond + Rfirst))

# Driver program
if __name__ == "__main__":
	input = 'GeeksforGeeks'
	d=2
	rotate(input,d)

'''
'''
x=str(input('enter the name'))
a=x[:2]
b=x[2:]
print(a)
print(b)
print('right rotation',b+a)
'''


#28.	String slicing in Python to check if a string can become empty by recursive deletion




























#29.	Python Counter| Find all duplicate characters in string
#30.	Python – Replace all occurrences of a substring in a string
