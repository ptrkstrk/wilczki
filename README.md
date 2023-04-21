# Requirements

Install [`wbgapi`](https://pypi.org/project/wbgapi/1.1.2/):

```bash
pip install wbgapi
```

# Downloading files
Run [`indicators.py`](./indicators.py):

```bash
python3 indicators.py
```
Skrypt pobiera mapę oraz wartości indykatorów i zapisuje je w katalogu `./data`. Logi pobierania zapisywane są w katalogu `./logs`. Dane kiva loans trzeba ręcznie pobrać z kaggle'a


# Zapisywanie w hdfs
Wszystkie dane przenosimy do katalogu, który jest współdzielony z kontenerem master. Do tego katalogu wrzucamy też skrypt `hdfs-store.sh`, który zapisze dane w systemie hdfs. Następnie wewnątrz kontenera odpalamy
```
hdfs-store.sh <data_input_path> <data_output_path>
```
- `<data_input_path>` - katalog, w którym są nasze dane
- `<data_input_path> - katalog, który chcemy utworzyć w systemie hdfs i zapisać w nim nasze dane 
