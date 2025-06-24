# get_proto

GET DVR 개발을 위한 HMI 소스 코드 원본 (by ZARIS)

## 펌웨어 설치

1. node js
2. mariaDB
3. InfluxDB (version 1)
4. midori


## 의존성 설치

<pre>
  npm install express 
  npm install influx
  npm install mariadb 
  npm install cors
  npm install modbus-serial </pre>


## 서버 실행

systemctl 상에 등록 후 실행 가능
<pre> sudo systemctl start get_server </pre>

## 앱 실행

nginx 에 <code> dist/browser/index.html </code> 등록 후 사용 가능


## 브라우저

midori 브라우저 사용
