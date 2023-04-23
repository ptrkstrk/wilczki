import wbgapi as wb
from time import perf_counter_ns, strftime
import os
import requests
from zipfile import ZipFile
from io import BytesIO

MAP_URL = "https://forobs.jrc.ec.europa.eu/data/products/gam/access_50k.zip"
LOG_FILE = f'./logs/logs_{strftime("%Y_%m_%d-%H_%M_%S")}.txt'
map_dir_name = './data/map'
ind_out_file_name = './data/indicators.csv'
os.mkdir('./data')
os.mkdir('./logs')

indicators = [
    "NY.GNP.PCAP.PP.CD",
    "SE.XPD.PRIM.PC.ZS",
    "SE.XPD.SECO.PC.ZS",
    "SE.XPD.TERT.PC.ZS",
    "IC.FRM.FEMO.ZS",
    "IC.REG.PROC.FE",
    "IC.REG.PROC.MA",
    "SL.TLF.CACT.FE.ZS",
    "SL.TLF.CACT.MA.ZS",
    "SL.EMP.MPYR.FE.ZS",
    "SL.EMP.MPYR.MA.ZS",
    "AG.LND.AGRI.ZS",
    "AG.LND.TOTL.K2",
    "PER_ALLSP.BEN_Q1_",
    "SL.UEM.TOTL.ZS",
    "EN.POP.SLUM.UR.ZS",
    "SI.DST.FRST.10",
]
year_range = range(2014, 2017)

def write_logs(msg):
    print(msg)
    with open(LOG_FILE, "a") as file:
        file.write(msg + "\n")


def download(download_cb, log_prefix):
    write_logs(f'{log_prefix}: starting download...')
    start_download_time = perf_counter_ns()
    data = download_cb()
    finish_download_time = perf_counter_ns()
    download_time = (finish_download_time - start_download_time) / 1000000000
    write_logs(f'{log_prefix}: download finished in {download_time:.2f} seconds')
    return data

write_logs(f'INDICATORS: downloading data for years {year_range.start}-{year_range.stop}')
write_logs(f'INDICATORS: downloading data for indicators: {indicators}')

data = download(lambda : (
    wb.data.DataFrame(
        indicators, time=year_range, skipBlanks=False, labels=True, columns="series"
    )
    .reset_index()
    .drop("time", axis=1)
), "INDICATORS")

write_logs(f'INDICATORS: saving data to {ind_out_file_name}...')
data.to_csv(ind_out_file_name, index=False)
out_file_bytes = os.path.getsize(ind_out_file_name)
out_file_kbytes = out_file_bytes / 1000
write_logs(f'INDICATORS: {out_file_kbytes:.2f} kB of data saved.')

# Map
response = download(lambda : requests.get(MAP_URL, stream=True).content, "ACC_MAP")

write_logs('ACC_MAPS: unzipping...')
with ZipFile(BytesIO(response)) as zipfile:
    zipfile.extractall(map_dir_name)

write_logs(f'ACC_MAP: deleting metadata files...')
for f in os.listdir(map_dir_name):
    if(f != 'acc_50k.tif'):
        os.remove(map_dir_name + "/" + f)
out_file_bytes = os.path.getsize(map_dir_name + "/acc_50k.tif")
out_file_kbytes = out_file_bytes / 1000
write_logs(f'ACC_MAP: extracted map file size is {out_file_kbytes:.2f} kB')

