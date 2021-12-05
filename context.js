require('dotenv').config();
const { MongoClient } = require("mongodb");
const _ = require('lodash');
let client;
let db;

const getDb = async () => {
    if(!client) {
        console.log(process.env.MONGOURL);
        client = new MongoClient(process.env.MONGOURL);
        await client.connect();
        db = client.db(process.env.DB_NAME);
    }
    return db;    
};

const bulkOps = [];
const insertLine = (book, bookContext, context ) => {
    const queryFormat = convertToQueryFormat(context, 'context');
    console.log(book, bookContext, queryFormat);
    bulkOps.push({
        updateOne: {
            filter: {
                book,
                bookContext
            },
            update: {
                $set: queryFormat                
            }
        } 
    });
};

const contextify = (a) => {
    const pows = [12, 10, 7, 4, 0];
    return a.reduce((prev,cur,index) => {
        const i = pows.length - a.length + index;
        prev = prev+cur*Math.pow(10,pows[i]);
        return prev;
    },0);
};

const extractSequence = (bookContext) => {
    const context = bookContext.split('.');
    return contextify(context);
};


const writeToDB = async () => {
    const slices = [];
    let count = 1;
    let i = 0;
    while(i < bulkOps.length) {
        if(i > bulkOps.length) {
            break;
        }
        console.log(i, 2000*count);
        slices.push(bulkOps.slice(i, 2000*count));
        i = i+2000;
        count++;
    }
    i = 0;
    const db = await getDb();
    while(i < slices.length) {
        console.log('writing till');
        const curSlice = slices[i];
        console.log(curSlice[curSlice.length -1]);
        await db.collection('lines').bulkWrite(slices[i]);
        i++;
    }
    client.close();
};

const convertToQueryFormat = (context, root) => {
    const keys = _.keys(context);
    const query = keys.reduce((prev, cur) => {
        prev[`${root}.${cur}`] = context[cur];
        return prev;
    }, {});
    return query;
};


const extractBookContext = (bookContext) => {
    const context = bookContext.split('.');
    const levels = context.reduce((prev,cur,index) => {
        prev[`L${index+1}`] = +cur;
        return prev;
    },{});
    return levels;    
};

const getBookAndContext = async () =>{
    const db = await getDb();
    const all = await db.collection('lines').find({},{projection: {book: 1, bookContext: 1}}).toArray();
    return all;
};


const run = async () => {
    const lines = await getBookAndContext();
    let i = 0;
    while( i < lines.length) {
        const line = lines[i];
        const context = extractBookContext(line.bookContext);
        // console.log(context, line.bookContext);
        insertLine(line.book, line.bookContext, context);
        i++;
    }
    console.log('Analysis complete - inserting', bulkOps.length);
    await writeToDB();    
};

run();