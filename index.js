const https = require('https');
const AWS = require('aws-sdk');
const urlParse = require('url').URL;
const csvtojson = require('csvtojson');
const _ = require('underscore');

const OPENSEARCH_LOGIN = 'dsapdevuser';
const OPENSEARCH_PASS = 'D-sap@machinedata2022';
const OPENSEARCH_ENDPOINT = 'https://search-dsapplatform-dev-6f52pzftvapaof2cmil256on3y.eu-west-3.es.amazonaws.com';
const region = 'eu-west-3'


const filePath = './csv_downloads/Download?Path=%2FDataLogs%2Ffile_2_28Jan_unrenamed.csv';

const columnsToRename = {
    "field4":      "puissance" ,
    "field5":      "val_TT-401" ,
    "field6":      "val_TT-402" ,
    "field7":       "val_FT-501" ,
    "field8":       "val_TSH-401" ,
    "field9":       "val_LIT-101" ,
    "field10":      "val_LIT-102" ,
    "field11":       "val_TT-101" ,
    "field12":       "val_TT-201" ,
    "field13":       "val_PT-101",
    "field14":       "systeme_mode_automatique" ,
    "field15":       "systeme_mode_manuel" ,
    "field16":       "systeme_mode_test" ,
    "field17":       "systeme_mode_arr�t" ,
    "field18":       "Torch�re_active_niveau_Haut_gaz" ,
    "field19":       "systeme_arr�t_urgnce" ,
    "field20":       "phase_compostage_digesteur_3" ,
    "field21":       "phase_anoxie_digesteur_3" ,
    "field22":       "phase_methanisation_digesteur_3",
    "field23":       "phase_methanisation_sans_percolation_digesteur_3" ,
    "field24":      "phase_inertage_digesteur_3" ,
    "field25":       "phase_inertage_digesteur_4" ,
    "field26":       "d�gonflage_joints_digesteurs_3" ,
    "field27":       "vidange_digesteur_3" ,
    "field28":       "percolation_digesteur_3" ,
    "field29":       "attente_apres_percolation_digesteur_3"  ,

    "field30":       "etape_initiale_grafcet_digesteur_4" ,
    "field31":       "phase_compostage_digesteur_4" ,
    "field32":       "phase_anoxie_digesteur_4" ,
    "field33":       "phase_methanisation_sans_percolation_digesteur_4" ,
    "field34":       "phase_inertage_digesteur_4" ,
    "field35":       "phase_inertage_digesteur_4" ,
    "field36":       "vidange_digesteur_4" ,
    "field37":       "percolation_digesteur_4" ,
    "field38":       "phase_compostage_digesteur_1" ,
    "field39":       "phase_anoxie_digesteur_1" ,

    "field40":       "phase_anoxie_digesteur_1" ,
    "field41":       "phase_methanisation_digesteur_1" ,
    "field42":       "phase_methanisation_sans_percolation_digesteur_1" ,
    "field43":       "phase_inertage_digesteur_1"  ,
    "field44":       "phase_inertage_digesteur_1" ,
    "field45":       "vidange_digesteur_1" ,
    "field46":      "percolation_digesteur_1" ,
    "field47":        "phase_compostage_digesteur_2" ,
    "field48":        "phase_anoxie_digesteur_2" ,

    "field49":        "phase_methanisation_digesteur_2",
    "field50":          "phase_methanisation_sans_percolation_digesteur_2", 
    "field51": "phase_inertage_digesteur_2", 
    "field52": "phase_inertage_digesteur_2", 
    "field53": "percolation_digesteur_2" 

}


const columnsParsing = {
    'Record': 'string',  'Date': 'string',      'UTC Time': 'string', 'field4': 'number',
    'field5': 'number',  'field6': 'number',    'field7': 'number',   'field8': 'number',
    'field9': 'number',  'field10': 'number',   'field11': 'number',  'field12': 'number',
    'field13': 'number', 'field14': 'number',   'field15': 'number',  'field16': 'number',
    'field17': 'number', 'field18': 'number',   'field19': 'number',  'field20': 'number',
    'field21': 'number', 'field22': 'number',   'field23': 'number',  'field24': 'number',
    'field25': 'number', 'field26': 'number',   'field27': 'number',  'field28': 'number',
    'field29': 'number', 'field30': 'number',   'field31': 'number',  'field32': 'number',
    'field33': 'number', 'field34': 'number',   'field35': 'number',  'field36': 'number',
    'field37': 'number', 'field38': 'number',   'field39': 'number',  'field40': 'number',
    'field41': 'number', 'field42': 'number',   'field43': 'number',  'field44': 'number',
    'field45': 'number', 'field46': 'number',   'field47': 'number',  'field48': 'number',
    'field49': 'number', 'field50': 'number',   'field51': 'number',  'field52': 'number',
    'field53': 'number'
    }
csvtojson({delimiter: "auto", trim:false, colParser:columnsParsing}).fromFile(filePath)
  .then(jsonArray => {
    // Rename columns
    jsonArrayRenamed = _.map(jsonArray, d => {
        _.map(columnsToRename, function(key, val)  {
            // console.log(key, val);
            d[key]=d[val];
            delete d[val];

        });
        return d;
    });  
    console.log(jsonArrayRenamed[0]);
    //concatenate Date + hour
    jsonArrayRenamed.forEach((row)=>{
        // console.log(row['Date']);
        var date = row['Date']?.replaceAll('/', '-');
        // console.log(date);
        var time = row['UTC Time'];
        // console.log(time);
    try{
        var d =  new Date(date+ ' '+ time+ ' UTC').toISOString();
        // console.log(d);
        row['timestamp'] = d;}
    catch(err) {
        console.log(err);
        row['timestamp'] = `${date} ${time}`; }
    });
    //Check
    // console.log(Object.keys(jsonArray[0])); 
    // console.log(jsonArray[0]);

    resp =  postDataES(OPENSEARCH_ENDPOINT, {'id': 'methania_plc'}, jsonArrayRenamed);
    console.log(resp);
  })
  .catch(err => {
    // log error if any
    console.log(err)
  })


const postDataES = async (esUrl, machine, data, chunkSize = 20) => {

  const chunks = chunksSplit(data, chunkSize);
  let response = [];
  for await (const chunk of chunks){
    
    let dataBody = "";
    chunk.map(d => {
      var doc_id = d.Record.replaceAll(" ", "")
      // console.log(doc_id);
      dataBody+=`{ "index": { "_index": "machine2_${machine.id}", "_id": ${doc_id}} } }\n`;
      dataBody+=`${JSON.stringify(d)}\n`;
    });
    dataBody+=`\n`;
    const responseChunk = await requestES(esUrl, {method: "POST", path: "/_bulk",body: dataBody});
    response = [...response, ...responseChunk.items];
  } 
  return response;
}


const chunksSplit = (array , size = 10) => {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    const chunk = array.slice(i, i + size);
    chunks.push(chunk);
  }
  return chunks;
}

const requestES = async (esUrl, options = {}) => {
  const req = new AWS.HttpRequest(esUrl, region);
  const endpoint = new urlParse(esUrl).hostname.toString()

  req.method = options.method || 'GET'
  req.path = options.path || '/'
  req.headers.host = endpoint
  req.headers['Content-Type'] = options.contentType ||  'application/json';
  const auth = 'Basic ' + Buffer.from( `${OPENSEARCH_LOGIN}:${OPENSEARCH_PASS}`).toString('base64');
  req.headers['Authorization'] = auth;
  req.body = options.body || req.body;

  return await new Promise((resolve, reject) => {
    const httpRequest = https.request({ ...req, host: endpoint }, (result) => {
      let data = "";
      result.on('data', (chunk) => {
        console.log("chunk layer requestES : ", chunk);
        data += chunk;        
      });
      result.on('end', () => {
        console.log("data layer requestES : ", data);
        const dataString = data.toString();
        console.log("dataString layer requestES : ", dataString);
        const dataObject = JSON.parse(dataString);
        console.log("dataObject layer requestES : ", dataObject);
        resolve(dataObject)
      });
    })

    httpRequest.write(req.body || "")
    httpRequest.end()
  })
}