import React from 'react';
import './App.scss';
import 'rsuite/dist/styles/rsuite-dark.css';
import { NavigationSidebar } from './navigation';

const App = (): JSX.Element => {
  return (
    <div className="App">
        <footer>
            <NavigationSidebar/>
        </footer>
    </div>
  );
}

export default App;
