const request = require('request');
const _ = require('lodash');
const csv=require('csvtojson');

const key = '';
const csvFilePath='./data.csv';
const  events = [];

csv().fromFile(csvFilePath) .on('json',(jsonObj) =>{
    events.push(jsonObj);
})
.on('done',(error)=> {
    const dataString = 'api_key='+ key + '&event=' + JSON.stringify(_.slice(events, [0], [1000]));
    const options = {
        url: 'https://api.amplitude.com/httpapi',
        method: 'POST',
        body: dataString
    };
    request(options,(error, response, body) => {
        if (error) {
          return console.error('upload failed:', error);
        }
        console.log('Upload successful!  Server responded with:', body);
    });
})


