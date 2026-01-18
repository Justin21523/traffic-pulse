const winston = require('winston');
// ...
axios.get('https://api.tdx.com/data', {
  // ...
})
.then(response => {
  const data = response.data;
  winston.info(`Received API response: ${JSON.stringify(data)}`);
  // Process the scraped data here
})
.catch(error => {
  winston.error(`Error making API request: ${error.message}`);
});
