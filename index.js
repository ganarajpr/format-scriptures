require('dotenv').config();
const { MongoClient } = require("mongodb");
const fs = require("fs");
const _ = require('lodash');
let client;
let db;
const Sanscript = require('@sanskrit-coders/sanscript');
const getDb = async () => {
    if(!client) {
        console.log(process.env.MONGOURL);
        client = new MongoClient(process.env.MONGOURL);
        await client.connect();
        db = client.db(process.env.DB_NAME);
    }
    return db;    
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

const bulkOps = [];
const insertLine = (book, bookContext, line) => {
    if(!/^(\d+\.)+\d+$/.test(bookContext)) {
        throw Error('Book Context is not in proper format ' + bookContext);
    }
    const context = convertToContextLevels(bookContext);
    bulkOps.push({
        insertOne: {
            document: {
                text: line,
                language: "sanskrit",
                script: "devanagari",
                book,
                bookContext,
                sequence: extractSequence(bookContext),
                context,
                createdBy: "112183819311352500254",
                createdAt: new Date(),
                gretil: true
            }
        } 
    });
}

const writeToDB = async () => {
    const slices = [];
    let count = 1;
    let i = 0;
    const maxWrite = 6000;
    while(i < bulkOps.length) {
        if(i > bulkOps.length) {
            break;
        }
        console.log(i, maxWrite*count);
        slices.push(bulkOps.slice(i, maxWrite*count));
        i = i+maxWrite;
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

const getBookContext = (ctx) => {
    try {
        let final = ctx.replace(/\[/g, '');
        final = final.replace(/\]/g, '');
        final = final.trim();
        return final;
    } catch(e) {
        console.log('Error context is :',ctx);
        throw e;
    }
}

const getVerse = (line) => {
    let vrs = line.replace('|', '\n');
    return vrs;
};

const convertToContextLevels = (bookContext) => {
    const context = bookContext.split('.');
    const levels = context.reduce((prev,cur,index) => {
        prev[`L${index+1}`] = +cur;
        return prev;
    },{});
    return levels;    
};

const extractBookContext = (line) => {
    const matches = line.match(/^[0-9]*\..*/);
    let ctx = null;
    let verse = null;
    if(matches) {
        ctx = getBookContext(matches[0]);
    } else {
        verse = getVerse(line);
    }
    if(ctx && !/^(\d+\.)+\d+$/.test(ctx)) {
        throw Error('Book Context is not in proper format ' + ctx + ' ' + matches[0]);
    }
    return {
        ctx,
        verse
    };
};

const extractTextFileContents = (file) => {
    let book = fs.readFileSync(file,'utf8');
    return book.split('\n');
};


const onContextAtEnd = (book, lines) =>{
    let i = 0;
    const data = [];
    let curContext = null;
    while( i < lines.length) {
        const line = lines[i].trim();
        if(line.length === 0) {
            i++;
            continue;
        }
        const { ctx, verse } = extractBookContext(line);
        if(ctx == null) {
            data.push(verse);
        } else {
            curContext = ctx;
            console.log(curContext);
            data.push(verse);
            const segment = Sanscript.t(data.join('\n'), 'iast', 'devanagari');
            console.log(segment);
            insertLine(book, curContext, segment);
            data.length = 0;
        }
        i++;
    }
};

const onContextAtBeginning = (book, lines) =>{
    let i = 0;
    const data = [];
    let curContext = null;
    while( i < lines.length) {
        const line = lines[i].trim();
        if(line.length === 0 || /^(\d+\.)+$/.test(line)) {
            console.log('continuing since ', line);
            i++;
            continue;
        }
        const { ctx, verse } = extractBookContext(line);
        if(ctx == null) {
            data.push(verse);
        } else {
            if(curContext === null) {
                curContext = ctx;    
            }            
            if(curContext !== ctx) {
                if(!data.length) {
                    throw new Error('no verse for', curContext);
                }
                const segment = Sanscript.t(data.join('\n'), 'iast', 'devanagari');
                insertLine(book, curContext, segment);
                console.log(curContext);
                console.log(segment);
                data.length = 0;
                curContext = ctx;
            }
            if(verse) {
                data.push(verse);
            }            
        }
        i++;
    }

    if(data.length) {
        const segment = Sanscript.t(data.join('\n'), 'iast', 'devanagari');
        insertLine(book, curContext, segment);
        console.log(curContext);
        console.log(segment);
    }
};

const run = async () => {
    const book ='zatapata brAhmaNa';
    const lines = extractTextFileContents('./sb14.txt');
    console.log('lines', lines.length);
    // onContextAtEnd(book, lines);
    onContextAtBeginning(book, lines);
    console.log('Analysis complete - inserting', bulkOps.length);
    await writeToDB();    
};

run();