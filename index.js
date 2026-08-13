const express = require('express');
const app = express();
__path = process.cwd()
const bodyParser = require("body-parser");
const PORT = process.env.PORT || 8000;
let code = require('./sakura'); 

require('events').EventEmitter.defaultMaxListeners = 500;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use('/code', code);

// All HTML pages live inside the "sakura" folder.
const SAKURA_DIR = __path + '/sakura';

app.get('/', (req, res) => {
    res.sendFile(SAKURA_DIR + '/main.html')
});
app.get(['/main', '/main.html'], (req, res) => {
    res.sendFile(SAKURA_DIR + '/main.html')
});
app.get(['/pair', '/pair.html'], (req, res) => {
    res.sendFile(SAKURA_DIR + '/pair.html')
});
app.get(['/settings', '/settings.html'], (req, res) => {
    res.sendFile(SAKURA_DIR + '/settings.html')
});

app.listen(PORT, () => {
    console.log(`
Don't Forget To Give Star ‼️


Server running on http://localhost:` + PORT)
});

module.exports = app;
