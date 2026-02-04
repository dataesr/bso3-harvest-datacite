dans /data rm -rf dois
mkdir temp2025
cd temp2025
wget "https://datafiles.datacite.org/datafiles/public-2025/download?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6ZmFsc2UsImlhdCI6MTc3MDIwMDg1NCwianRpIjoiNmQ5NGE1YTUtMDIyZC00ZDhmLWE1ODctZGMwODY0NTQ4MTM3IiwidHlwZSI6ImFjY2VzcyIsInN1YiI6MTE3MywibmJmIjoxNzcwMjAwODU0LCJjc3JmIjoiNGY2MTZkYTQtNDg0Ny00NzkzLThjZTEtYjQ1ZTM1ZjEzM2Y4IiwiZXhwIjoxNzcwMjg3MjU0fQ.h1KMsl5CfnRbTBAwfr0cJkmdu7Xik2pF6FcdIl2cPCM" -O datacite_2025.tar &
tar -xvf datacite_2025.tar &
mv dois ../.
cd ..
rm -rf temp2025
