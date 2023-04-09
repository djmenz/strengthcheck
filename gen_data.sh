# remove current datafiles
rm openipf-latest.csv
rm openpowerlifting-latest.csv

cd temp_data

wget https://openpowerlifting.gitlab.io/opl-csv/files/openipf-latest.zip
wget https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip

unzip openipf-latest.zip
unzip openpowerlifting-latest.zip

cd ..

mv temp_data/openipf*/*.csv openipf-latest.csv
mv temp_data/openpowerlifting*/*.csv openpowerlifting-latest.csv

### cleanup remove directories and zip files
rm -rf temp_data/*

python3 /Users/djmenz/Documents/opl/strengthcheck/processer.py gen; 
cp /Users/djmenz/Documents/opl/strengthcheck/all_data.json /Users/djmenz/Documents/opl/strengthcheck-ui/all_data.json

cd /Users/djmenz/Documents/opl/strengthcheck-ui

# fix up date
sed "111s/.*/<strong>Last Update $(date +"%d %B %Y") - This page relies on data/" src/App.vue > src/App.vue.new
mv src/App.vue.new src/App.vue
rm src/App.vue.new

# build distribution
npm run build

# run this manually to do the actual sync
aws s3 sync /Users/djmenz/Documents/opl/strengthcheck-ui/dist s3://jcore.rocks

# required python packages
# pandas
# simple_term_menu
# and npm / npm install
