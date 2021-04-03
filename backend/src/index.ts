import express from 'express';
import {Db, MongoClient} from 'mongodb';

// Setup express
const app = express();
const port = 3031; // Default port to listen

// Setup MongoDB
const isset = process.env.MONGO_INITDB_USERNAME && process.env.MONGO_INITDB_PASSWORD;
const DB_URI = `mongodb://${isset ? (process.env.MONGO_INITDB_USERNAME + ':' + process.env.MONGO_INITDB_PASSWORD + '@') : ''}${process.env.MONGO_HOSTNAME}:${process.env.MONGO_PORT}/${process.env.MONGO_INITDB_DATABASE}`;
const client = new MongoClient(DB_URI);

const getAllCards = (db: Db, searchParam: string, callback: (cards: any) => void) => {
    const regex = RegExp(`.*${searchParam}.*`, 'i');
    const collection = db.collection('cards');
    collection.find({$or: [{name: regex}, {text: regex}, {artist: regex}]}).toArray((err, cards) => {
        callback(cards);
    });
}

app.get('/cards', (req, res) => {
    const searchParam = req.query.searchParam as string;
    client.connect((err: any) => {
        if (err) {
            console.log('Failed to connect to mongodb.')
            return;
        }
        console.log('Connected to mongodb');
        const db = client.db('mtg');

        getAllCards(db, searchParam, (cards: any) => {
            res.header('Access-Control-Allow-Origin', '*');
            res.json({'cards': cards.slice(0, 20)})
        });
    });
});

// Start the Express server
app.listen(port, () => {
    console.log( `server started at http://localhost:${port}` );
});
