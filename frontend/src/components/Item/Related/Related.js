import { useState, useEffect } from 'react';
import axios from 'axios'; 
import RelatedCard from '../../Card/RelatedCard/RelatedCard';
import { API_ENDPOINT } from '../../../config.js';
import './Related.css';

const Related = (props) => {
    
    const [ items, seItems ] = useState()

    useEffect(() => {
        axios.get(`${API_ENDPOINT}/retail/featured`, {
            headers: {
              'Access-Control-Allow-Origin': '*'
            }
          })
            .then(res => {
                seItems(res.data)
            })
            .catch(err => console.log(err))
    }, [])
    
    return ( 
            <div className="related__products">
                <div className="related__header__container">
                    <div className="related__header">
                        <h2>Recommended Products</h2>
                    </div>
                    <div className="related__header__line">
                            
                    </div>
                </div>
                <div className="related__card__container">
                    <div className="related__product__card">
                        { items && items.map((item) => <RelatedCard item={item}/>)}
                    </div>
                </div>
            </div>
     );
}
 
export default Related;