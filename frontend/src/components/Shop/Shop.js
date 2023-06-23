import { useEffect, useState } from 'react';
import { TabTitle } from '../../utils/General';
import axios from "axios";
import ShopCategory from './Container/ShopCategory';
import './Shop.css';
import ReactLoading from 'react-loading';
import { API_ENDPOINT } from '../../config.js';

const Shop = () => {
    TabTitle("Shop - LEAFYY")
    const [ menItems, setMenItems ] = useState()
    const [ loading , setLoading ] = useState(true) 

    useEffect(() => {
        axios.get(`${API_ENDPOINT}/retail/featured`, {
            headers: {
              'Access-Control-Allow-Origin': '*'
            }
          })
            .then(res => {

                setMenItems(res.data)
                setLoading(false)
            })
            .catch(err => console.log(err))
        window.scrollTo(0, 0)
    
    }, [])

    return ( 
        <div className="shop__contianer">
            {loading && <ReactLoading type="balls" color='#dbf3dd'  height={100} width={100} className='container h-100 w-10 justify-self-center align-self-center m-auto'/>}
            {menItems && <ShopCategory name="Men" key="men" items={menItems}/>}
            {/* {womenItems && <ShopCategory name="Women" key="women" items={womenItems}/>}
            {kidsItems && <ShopCategory name="Kids" key="kids" items={kidsItems}/>} */}
        </div>
     );
}
 
export default Shop;