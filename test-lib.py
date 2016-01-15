from s3file import s3open

f = s3open("https://s3.amazonaws.com/s3file_075684_11114/serkantest2.txt")
f.write("Lorem ipsum dolor sit amet...")
f.close()
