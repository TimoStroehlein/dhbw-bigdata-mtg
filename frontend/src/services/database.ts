import {Db, MongoClient} from 'mongodb';

export const getCards = () => {
    const dbUri = getUri();
    console.log(dbUri);
    MongoClient.connect(dbUri, (err, client) => {
        if (err) {
            console.log('Failed to connect to mongodb on startup - retrying in 5 seconds.')
        }
        console.log('Connected to mongodb');
        const db = client.db('mtg');

        getAllCards(db, (cards: any) => {
            console.log(cards);
        });
    });
}

const getUri = (): string => {
    const dbUri = 'mongodb://dev:dev@mongo:27017/mtg';
    return dbUri;
}

const connectWithRetry = (dbUri: string) => {
    MongoClient.connect(dbUri, (err, client) => {
        if (err) {
            console.log('Failed to connect to mongodb on startup - retrying in 5 seconds.')
            setTimeout(connectWithRetry, 5000);
        }
        console.log('Connected to mongodb');
        const db = client.db('mtg');

        getAllCards(db, (cards: any) => {
            console.log(cards);
        });
    });
}

const getAllCards = (db: Db, callback: (cards: any) => void) => {
    const collection = db.collection('cards');
    collection.find({}).toArray((err, cards) => {
        console.log(cards);
        callback(cards);
    });
}
