const express = require('express');
const app = express();
const bodyParser = require('body-parser');
const csv = require('csvtojson');
const alasql = require('alasql');
const Moment = require('moment');
const MomentRange = require('moment-range');
const moment = MomentRange.extendMoment(Moment);

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
          time: moment(Number(jsonObj.time)).format('MM/DD/YYYY') // Set it to the beginning of the day
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

app.get('/cohort', (req, res) => {
  const { filename, frecuency } = {filename: 'effortless.csv', frecuency: 'weekly'};

  csv2json(filename)
    .then((data) => {
      // Order the response by the date from older to newer
      const range = moment.range('2017-10-23', '2017-12-11'); 
      let weeks = [];   
        for (let month of range.by('week')) {
            weeks.push(month.format('MM/DD/YYYY'));
        }
       const cohort1 = 'SELECT DISTINCT distinct_id from ? WHERE time BETWEEN "'+ weeks[0] +'" AND "'+ weeks[1] +'" GROUP BY distinct_id ORDER BY time ASC';
       const cohort2 = alasql('SELECT DISTINCT distinct_id from ? WHERE time BETWEEN "'+ weeks[1] +'" AND "'+ weeks[2] +'" AND distinct_id IN ('+ cohort1 + ') GROUP BY distinct_id ORDER BY time ASC', [data])
       res.json(cohort2);
    })
    .catch((error) => {
      console.error(error);
      res.status(500).send('Something broke!');
    })
});

app.listen(3000, () => console.log('Example app listening on port 3000!'));