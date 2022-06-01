# Get latest data and build new ui and deploy (must be run in /strengthcheck folder)
rm openipf-latest.zip
rm openpowerlifting-latest.zip
rm openipf-latest.csv
rm openpowerlifting-latest.csv

wget https://openpowerlifting.gitlab.io/opl-csv/files/openipf-latest.zip
wget https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip

unzip openipf-latest.zip
unzip openpowerlifting-latest.zip
mv openipf*/*.csv openipf-latest.csv
mv openpowerlifting*/*.csv openpowerlifting-latest.csv

### cleanup remove directories and zip files
python3 /home/daniel/Documents/opl/strengthcheck/processer.py gen; 
cp /home/daniel/Documents/opl/strengthcheck/all_data.json /home/daniel/Documents/opl/strengthcheck-ui/all_data.json

### need to figure out how to run this command from any directory
# change date in aboutview.vue
cd /home/daniel/Documents/opl/strengthcheck-ui
npm run build
# aws s3 sync /home/daniel/Documents/opl/strengthcheck-ui/dist s3://jcore.rocks
