# near-near-map-function-dynamodb-stream

## ライブラリのインストール

```
$ pip install -r requirements_xxx.txt -t source_xxx
```


## パッケージング&デプロイ コマンド

```
$ find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
$ cd source_xxx
$ zip -r ../lambda-package.zip *
$ aws lambda update-function-code --function-name near-near-map-dynamodb2athena --zip-file fileb://../lambda-package.zip
```
