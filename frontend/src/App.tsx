import React from 'react';
import './App.scss';
import 'rsuite/dist/styles/rsuite-dark.css';
import {HomeScreen} from './screens/Home';


const App = (): JSX.Element => {
  return (
    <div className="App">
        <footer>
            <HomeScreen/>
        </footer>
    </div>
  );
}

export default App;
