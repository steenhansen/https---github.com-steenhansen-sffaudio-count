
const express = require('express')
const mongoose = require('mongoose')
const mongooseToCsv = require('mongoose-to-csv-quotes') 
const fs = require('file-system')
mongoose.Promise = require('bluebird')

const DAYS_SIGNIFIER=28 
const TYPE_PDF_CAN = 'can_pdf'
const TYPE_PDF_USA = 'usa_pdf'
const TYPE_RSD = 'rsd_podcast'
const TYPE_SFF_CAN = 'can_podcast'
const TYPE_SFF_USA = 'usa_podcast'
const MEDIA_TYPES = [TYPE_PDF_CAN, TYPE_PDF_USA, TYPE_RSD, TYPE_SFF_CAN, TYPE_SFF_USA]
const TYPE_OUTPUT = 'csv_output'

const CANADA_AMAZON_DIR = 'http://sffaudiomediacan.s3.amazonaws.com/canadianpodcasts/'
const USA_DO_RSD = 'https://nyc3.digitaloceanspaces.com/sffaudio-usa/rsd-podcasts/'
const USA_DO_SFF = 'https://nyc3.digitaloceanspaces.com/sffaudio-usa/podcasts/'
const CAN_PDF_URI = 'http://sffaudiomediacan.s3.amazonaws.com/pdfs/'
const USA_PDF_URI = 'http://nyc3.digitaloceanspaces.com/sffaudio-usa/usa-pdfs/'

const model_name = 'media_downloads'

// To get connection address: $ heroku config:get MONGODB_URI -a __ur__app__name__
// const mongo_connect = "mongodb://heroku_abcdefgh:12345678901234567890123456@ab123456.mlab.com:12345/heroku_abcdefgh"

const mongo_connect = 'mongodb://localhost:27017/'+ model_name
mongoose.connect(mongo_connect, {useMongoClient: true})

const document_schema = {
  'media_name': String,
  'download_count': Number,
  'download_year': Number,
  'download_month': Number,
  'usa_dd_mm_yyyy': String, 
  'yyyy_mm_dd': String, 
  'media_type': String
}

media_schema = new mongoose.Schema(document_schema)
const header_values = 'media_name download_count download_year download_month usa_dd_mm_yyyy yyyy_mm_dd media_type'
media_schema.plugin(mongooseToCsv, { headers: header_values })
const media_model = mongoose.model(model_name, media_schema)

function yearMonth (){
  const now_date = new Date()
  const now_year = now_date.getFullYear()
  const now_month = now_date.getMonth() + 1
  return [now_year, now_month]
}

function initDownload(media_type, media_name){
  const [now_year, now_month]=yearMonth()
  const the_day = DAYS_SIGNIFIER
  const now_usa = the_day + '/' + now_month + '/' + now_year
  const now_metric = now_year + '-' + now_month + '-' + the_day
  const first_download = {
    'media_type': media_type,  
    'media_name': media_name,
    'usa_dd_mm_yyyy': now_usa, 
    'yyyy_mm_dd': now_metric, 
    'download_year': now_year,
    'download_month': now_month,
    'download_count': 1 
  }
  return first_download
}

function updateDownload(find_model){
  find_model.download_count = find_model.download_count + 1
  find_model.save()
}

function countDownload(media_model, media_type, media_name){
  const [now_year, now_month]=yearMonth()
  const type_name_year_month = { 'media_type' : media_type, 'media_name': media_name, 'download_year':now_year, 'download_month':now_month }
  const return_values = 'download_count'
  media_model.findOne(type_name_year_month, return_values,  function (err, find_model) {
    if (find_model===null) {
      if (MEDIA_TYPES.includes(media_type)){
        const init_download = initDownload(media_type, media_name)
        media_model.create(init_download)
      }
    }else{
      updateDownload(find_model)
    }
  })
}

function figureFilter(filter_type){
  if (typeof filter_type ==='undefined'){
    return [undefined, undefined]
  }
  const parsed_type = parseInt(filter_type, 10)
  if (isNaN(parsed_type)) { 
    if (filter_type===''){
      return [undefined, undefined]
    }else{
      return ['media_type', filter_type] 
    }
  } else if (parsed_type>12){
    return ['download_year', parsed_type] 
  }else{
    return ['download_month', parsed_type] 
  }
}

function outputCsv(filter_1, filter_2, filter_3){
  let filter_types = {}
  const [type_filter_1, value_filter_1]=figureFilter(filter_1)
  if (typeof type_filter_1 !== 'undefined'){
    filter_types[type_filter_1]=value_filter_1    
  }
  const [type_filter_2, value_filter_2]=figureFilter(filter_2)
  if (typeof type_filter_2 !== 'undefined'){
    filter_types[type_filter_2]=value_filter_2    
  }
  const [type_filter_3, value_filter_3]=figureFilter(filter_3)
  if (typeof type_filter_3 !== 'undefined'){
    filter_types[type_filter_3]=value_filter_3    
  }
  return filter_types
}

const countFilenames = function (req, res) {
  const [_slash_var_, file_type, filename_or_type, type_2, type_3] = req.originalUrl.split('/')
  if (file_type===TYPE_OUTPUT){
    const filter_types= outputCsv(filename_or_type, type_2, type_3)
    media_model.findAndStreamCsv(filter_types).pipe(res)
  } else{
    countDownload(media_model, file_type, filename_or_type)     
    if (file_type===TYPE_PDF_CAN){
      const canadian_pdf = CAN_PDF_URI + filename_or_type
      res.redirect(canadian_pdf)
    } else  if (file_type===TYPE_PDF_USA){
      const usa_pdf = USA_PDF_URI + filename_or_type
      res.redirect(usa_pdf)
    }else  if (file_type===TYPE_RSD){
      const usa_rsd = USA_DO_RSD + filename_or_type
      res.redirect(usa_rsd)
    }else  if (file_type===TYPE_SFF_CAN){
      const canadian_sff = CANADA_AMAZON_DIR + filename_or_type
      res.redirect(canadian_sff)
    }else  if (file_type===TYPE_SFF_USA){
      const usa_sff = USA_DO_SFF + filename_or_type
      res.redirect(usa_sff)
    }
  }
}

const server = express()
  .use(countFilenames)
  .listen(process.env.PORT || 5000)
