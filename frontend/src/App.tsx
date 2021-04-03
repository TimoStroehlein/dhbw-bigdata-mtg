import './App.scss';
import 'rsuite/dist/styles/rsuite-dark.css';
import {HomeScreen} from './screens/Home';


const App = (): JSX.Element => {
  document.title = 'Big Data - MTG API';

  return (
    <div className="App">
        <footer>
            <HomeScreen/>
        </footer>
    </div>
  );
}

export default App;
