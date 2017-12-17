const express = require('express');
const fileUpload = require('express-fileupload');
const app = express();
const bodyParser = require('body-parser');
const csv = require('csvtojson');
const alasql = require('alasql');
const Moment = require('moment');
const MomentRange = require('moment-range');
const moment = MomentRange.extendMoment(Moment);
const csv_out = require('express-csv');

alasql.fn.moment = moment;

/**
 * Wrapper method to convert CSV into JSON and format dates
 * @param filename
 * @returns {Promise}
 */
const csv2json = (data) => {
  return new Promise((success, reject) => {
    let csv_data = [];
    // Transform CSV into JSON
    csv()
      .fromFile(data)
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

/**
 * Recursive method to extract cohorts
 * @param weeks
 * @param cohorts
 */
const extract_cohorts = (weeks, data, cohorts, cohort_id) => {
  if(weeks.length === 0) return cohorts;

  // Extract the cohort unique IDs from week 0
  const result = alasql('SELECT DISTINCT distinct_id from ? WHERE time BETWEEN "'+ weeks[0] +'" AND "'+ weeks[1] +'" GROUP BY distinct_id ORDER BY time ASC', [data]);
  const unique_ids = result.map(obj => {
    return obj.distinct_id;
  });

  cohorts[cohort_id] = [];
  cohorts[cohort_id].push({week: weeks[0], count: unique_ids.length});

  weeks.splice(0, 1); // Delete the first week

  for(let [index, week] of weeks.entries()) {
    if (weeks[index + 1] === undefined) continue;

    const query_res = alasql('SELECT DISTINCT distinct_id from ? WHERE distinct_id IN ("'+(unique_ids.join('" , "'))+'") AND time BETWEEN "'+ weeks[index] +'" AND "'+ weeks[index + 1] +'" GROUP BY distinct_id ORDER BY time ASC', [data]);

    const ids = query_res.map(obj => {
      return obj.distinct_id;
    });

    cohorts[cohort_id].push({week: weeks[index + 1], count: ids.length});
  }
  extract_cohorts(weeks, data, cohorts, ++cohort_id);
};

// Body parser configuration
app.use( bodyParser.json() );
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
  extended: true
}));

app.use(fileUpload());

// Add headers
app.use(function (req, res, next) {
      // Website you wish to allow to connect
      res.setHeader('Access-Control-Allow-Origin', '*');
      // Request methods you wish to allow
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');
      // Request headers you wish to allow
      res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');
      // Set to true if you need the website to include cookies in the requests sent
      // to the API (e.g. in case you use sessions)
      res.setHeader('Access-Control-Allow-Credentials', true);
      // Pass to next layer of middleware
      next();
});

// Initial API Endpoint
app.get('/', (req, res) => {
  res.json({ api: 'V1.0', description: 'Cohorts API'});
});

app.post('/cohort', (req, res) => {
  if (!req.files)
  return res.status(400).send('No files were uploaded.');

  const body =  req.body;
  csv2json(req.files.data)
    .then((data) => {
      // Order the response by the date from older to newer
      const range = moment.range('2017-10-23', '2017-12-11'); 
      let weeks = [];   
      for (let month of range.by('week')) {
          weeks.push(month.format('MM/DD/YYYY'));
      }
      const cohorts = [];
      extract_cohorts(weeks, data, cohorts, 0);

      const final_cohorts = [];
      for(let cohort of cohorts) {
        let index_cohort = cohort[0].week;
        for(let cohort_data of cohort.entries()) {
          final_cohorts.push([index_cohort, cohort_data[1].week, cohort_data[1].count, 0]);
        }
      }
      res.csv(final_cohorts);
    })
    .catch((error) => {
      res.status(500).send('Something broke!');
    })
});

app.listen(process.env.PORT || 3000, ()=> {
  console.log('Example app listening on port 3000!');
});
