const express = require('express')
const cors = require('cors')
const dotenv = require('dotenv')


const app = express()

app.use(cors())

dotenv.config()

const port = process.env.PORT || 5000

let query = require('./query')

app.get('/api/v1/test', (req, res) => {
  res.send('Hello World!')
})

app.get('/api/v1/search', async (req, res) => {
  res.header("Access-Control-Allow-Origin", "*");
  let q = req.query.q
  let result = await query.search(q)
  res.send(result)
})

app.get('/api/v1/search_title', async (req, res) => {
  res.header("Access-Control-Allow-Origin", "*");
  let q = req.query.q
  let result = await query.searchByTitle(q)
  res.send(result)
})

app.listen(port, () => {
  console.log(`Listening on ${port}`)
})
