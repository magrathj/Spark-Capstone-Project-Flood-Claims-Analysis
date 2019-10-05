from zipfile import ZipFile
import glob
import sys, getopt

# path = 'C:\\Users\\jmagr\\Downloads\\policy'

# def main():

#     print("extracting all zip files in ")
#     files = [f for f in glob.glob(path + "**/*.zip", recursive=True)]

#     for file in files:
#         print(file)
#         with ZipFile(file, 'r') as zipObj:
#             zipObj.extractall(path)
#         print("--------------")
        

def main(argv):
   print(sys.argv[2])

if __name__ == "__main__":
   main(sys.argv[1:])
