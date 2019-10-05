from zipfile import ZipFile
import glob


path = 'C:\\Users\\jmagr\\Downloads\\policy'
files = [f for f in glob.glob(path + "**/*.zip", recursive=True)]

for file in files:
    print(file)
    with ZipFile(file, 'r') as zipObj:
        zipObj.extractall(path)
    print("--------------")
    

