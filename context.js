require('dotenv').config();
const { MongoClient } = require("mongodb");

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
const insertLine = (book, bookContext, sequence ) => {
    bulkOps.push({
        updateOne: {
            filter: {
                book,
                bookContext
            },
            update: {
                $set: { 
                    sequence
                }                
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
        const seq = extractSequence(line.bookContext);
        console.log(seq, line.bookContext);
        insertLine(line.book, line.bookContext, seq);
        i++;
    }
    console.log('Analysis complete - inserting', bulkOps.length);
    // await writeToDB();    
};

run();