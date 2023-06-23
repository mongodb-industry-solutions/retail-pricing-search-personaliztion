import { BrowserRouter as Router } from 'react-router-dom';
import { Route , Routes} from 'react-router-dom';

import './App.css';
import 'bootstrap/dist/css/bootstrap.min.css';
import Home from '../routes/Home';
import Header from '../components/Header/Header';
import Footer from '../components/Footer/Footer';
import Shop from '../components/Shop/Shop';
import ItemView from '../routes/ItemView';
import SearchView from '../routes/Search';
import CartItemsProvider from '../Context/CartItemsProvider';
import Login from '../components/Authentication/Login/Login';
import Register from '../components/Authentication/Register/Register';
import Wishlist from '../components/Wishlist';
import WishItemsProvider from '../Context/WishItemsProvider';
import SearchProvider from '../Context/SearchProvider';

function App() {

  return (
   <CartItemsProvider>
      <WishItemsProvider>
        <SearchProvider>
          <Router >
            <Header />
            <Routes>
              <Route path="/"  element={<Home />}/>
              <Route index element={<Home />}/>
              <Route path="/account">
                <Route path="login" element={<Login />}/>
                <Route path="register" element={<Register />}/>
                <Route path="*" element={<Login />}/>
              </Route>
              <Route path="/shop" element={<Shop />}/>
              <Route path="/product">
                <Route path=":id" element={<ItemView />}/>
              </Route>
              <Route path="/wishlist" element={<Wishlist />} />
              <Route path="/search/*" element={<SearchView />} />
            </Routes>
            <Footer />
            <Routes>
            <Route path="/admin" element={<Wishlist />} />
            </Routes>
          </Router>
        </SearchProvider>
      </WishItemsProvider>
   </CartItemsProvider>
  );
}

export default App;