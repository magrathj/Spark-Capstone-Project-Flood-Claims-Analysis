import urllib.request 
import progressbar


class MyProgressBar():
    def __init__(self):
        self.pbar = None

    def __call__(self, block_num, block_size, total_size):
        if not self.pbar:
            self.pbar=progressbar.ProgressBar(maxval=total_size)
            self.pbar.start()

        downloaded = block_num * block_size
        if downloaded < total_size:
            self.pbar.update(downloaded)
        else:
            self.pbar.finish()



print("-------starting-------")

urllib.request.urlretrieve("https://www.fema.gov/media-library-data/1568732098427-f65ec6f87d6f9e543b1f3ad479963ee1/FIMA_NFIP_Redacted_Policies_Data_Set_Part_1.zip", "C:/Users/jmagr/Downloads/policy/p1.csv.zip", MyProgressBar())

print("retrieved zip 1")

urllib.request.urlretrieve("https://www.fema.gov/media-library-data/1568732503790-cdfd3bb74070b87762a7335559b87397/FIMA_NFIP_Redacted_Policies_Data_Set_Part_2.zip", "C:/Users/jmagr/Downloads/policy/p2.csv.zip", MyProgressBar())

print("retrieved zip 2")

urllib.request.urlretrieve("https://www.fema.gov/media-library-data/1568732976578-43272b0f0ab2b303ec655a9052f86276/FIMA_NFIP_Redacted_Policies_Data_Set_Part_3.zip", "C:/Users/jmagr/Downloads/policy/p3.csv.zip", MyProgressBar())

print("retrieved zip 3")

urllib.request.urlretrieve("https://www.fema.gov/media-library-data/1568733224605-bba2aca01ef6124f1042e382072ca2aa/FIMA_NFIP_Redacted_Policies_Data_Set_Part_4.zip", "C:/Users/jmagr/Downloads/policy/p4.csv.zip", MyProgressBar())

print("retrieved zip 4")

urllib.request.urlretrieve("https://www.fema.gov/media-library-data/1568733677863-78f7f37fac6eadcde1fb97c096ffca14/FIMA_NFIP_Redacted_Policies_Data_Set_Part_5.zip", "C:/Users/jmagr/Downloads/policy/p5.csv.zip", MyProgressBar())

print("retrieved zip 5")

print("------completed-----------")
