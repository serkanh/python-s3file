from s3file import s3open

f = s3open("https://s3.amazonaws.com/s3file_075684_11114/serkantest4.txt")
#f.write("Lorem ipsum dolor sit amet...")
x = f.read()
# print(dir(x))
# print(str(x))
print(x)

f.close()
