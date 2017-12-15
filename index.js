const express = require('express');
const app = express();
const bodyParser = require('body-parser');
const csv = require('csvtojson');
const alasql = require('alasql');
const moment = require('moment');
alasql.fn.moment = moment;

const csv2json = (filename) => {
  return new Promise((success, reject) => {
    let csv_data = [];

    // Transform CSV into JSON
    csv()
      .fromFile(`./data/${filename}`)
      .on('json',(jsonObj)=>{
        csv_data.push({
          distinct_id: jsonObj.distinct_id,
          time: moment(parseInt(jsonObj.time)).startOf('day') // Set it to the beginning of the day
        });
      })
      .on('done',(error)=>{
        if(error) {
          reject(error);
        } else {
          success(csv_data);
        }
      })
  });
};


// Body parser configuration
app.use( bodyParser.json() );
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
  extended: true
}));

// Initial API Endpoint
app.get('/', (req, res) => {
  res.json({ api: 'V1.0', description: 'Cohorts API'});
});

app.post('/cohort', (req, res) => {
  const { filename, frecuency } = {filename: 'effortless.csv', frecuency: 'weekly'};

  csv2json(filename)
    .then((data) => {
      // Order the response by the date from older to newer
      const response = alasql('SELECT DISTINCT distinct_id, time from ? ORDER BY time ASC', [data]);
      res.json(response);
    })
    .catch((error) => {
      console.error(error);
      res.status(500).send('Something broke!');
    })
});

app.listen(3000, () => console.log('Example app listening on port 3000!'));