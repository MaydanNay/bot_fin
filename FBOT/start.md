git add .
git commit -m "m"
git push -f origin main

cd FBOT
docker-compose down
docker-compose up --build -d
docker-compose logs -f
