@echo off
echo ================================================
echo  Turkey Power Trading - Proje Kurulumu
echo ================================================

mkdir fetch
mkdir process
mkdir models
mkdir dashboards
mkdir data\raw
mkdir data\processed
mkdir notebooks
mkdir config
mkdir tests

copy .env.example .env
echo .env dosyasi olusturuldu - icerigini doldurmay unutma!

echo.
echo Python sanal ortam olusturuluyor...
python -m venv venv

echo.
echo Paketler yukleniyor...
call venv\Scripts\activate
pip install -r requirements.txt

echo.
echo ================================================
echo  Kurulum tamamlandi!
echo  Siradaki adim: .env dosyasini duzenle
echo ================================================
pause
