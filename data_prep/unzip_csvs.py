from zipfile import ZipFile
import glob
import sys, getopt
       

def main(argv):
   path = sys.argv[1]
   path_output = sys.argv[2]
   print("extracting all zip files in ", path)
   files = [f for f in glob.glob(path + "**/*.zip", recursive=True)]
   for file in files:
       print(file)
       with ZipFile(file, 'r') as zipObj:
           zipObj.extractall(path_output)
       print("--------------")

# Example python unzip_csvs.py 'C:\\Users\\jmagr\\Downloads\\policy' 'C:\\Users\\jmagr\\Downloads\\policy'
if __name__ == "__main__":
   main(sys.argv[1:])
