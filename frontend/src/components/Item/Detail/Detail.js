import { useContext, useState } from 'react';
import './Detail.css';
import { Button } from '@mui/material';
import { IconButton } from '@mui/material';
import AddCircleIcon from '@mui/icons-material/AddCircle';
import RemoveCircleIcon from '@mui/icons-material/RemoveCircle';
import { CartItemsContext } from '../../../Context/CartItemsContext';
import { WishItemsContext } from '../../../Context/WishItemsContext';
import FavoriteBorderIcon from '@mui/icons-material/FavoriteBorder';
import { API_ENDPOINT } from '../../../config.js';
import axios from 'axios';

const Detail = (props) => {
    const [quantity, setQuantity] = useState(1);

    const cartItems = useContext(CartItemsContext)
    const wishItems = useContext(WishItemsContext)
    
    const handelQuantityIncrement = (event) => {
        setQuantity((prev) => prev+=1);
    };

    const handelQuantityDecrement = (event) => {
        if(quantity >1){
            setQuantity((prev) => prev-=1);
        }
    };

    const handelAddToCart = () => {
        pushToCollection('cart',props.item.discountedPrice, props.item.id)
        cartItems.addItem(props.item, quantity)
    }

    const handelAddToWish = () => {
        pushToCollection('view',props.item.discountedPrice, props.item.id)
        wishItems.addItem(props.item)
    }

    const pushToCollection = (eventType, price, productId) => {
        const currentDate = new Date();
        const formattedDate = currentDate.toISOString().slice(0, 19).replace('T', ' ');
      
        const data = {
          topic: 'clogs',
          message: {
            event_time: `${formattedDate} UTC`,
            event_type: eventType,
            product_id: productId,
            price: price,
            user_session: generateRandomId(),
            count: 1
          }
        };
      
        axios.post(`${API_ENDPOINT}/retail/pushToCollection`, data)
          .then(response => {
            console.log(response.data);
          })
          .catch(error => {
            console.log(error);
          });
      };
      
      const generateRandomId = () => {
        return Math.random().toString(36).substring(2);
      };
    
    return ( 
        <div className="product__detail__container">
            <div className="product__detail">
                <div className="product__main__detail">
                    <div className="product__name__main">{props.item.title.slice(0, 30)}</div>
                    <div className="product__detail__description">{props.item.mfg_brand_name}</div>
                    <div className="product__color">
                        <div className="product-color-label">COLOR</div>
                        <div className="product-color" style={{backgroundColor: `brown`}}></div>
                    </div>
                    <div className="product__price__detail">
                        {props.item.atp > 0 ? (<span>₹{props.item.discountedPrice.toFixed(2)}</span>) : (<span className="product__sold-out">Sold Out</span>)}
                    </div>
                </div>
                <form onSubmit={handelAddToCart} className="product__form">
                <div className="product__quantity__and__size">
                    <div className="product__quantity">
                        <IconButton onClick={handelQuantityIncrement}>
                            <AddCircleIcon />
                        </IconButton>
                        <div type="text" name="quantity" className="quantity__input">{quantity}</div>
                        <IconButton onClick={handelQuantityDecrement}>
                            <RemoveCircleIcon fontSize='medium'/>
                        </IconButton>
                    </div>
                </div>  
                <div className="collect__item__actions">
                    <div className="add__cart__add__wish">
                        <div className="add__cart">
                            <Button variant="outlined" size="large" sx={[{'&:hover': { backgroundColor: '#dbf3dd', borderColor: '#dbf3dd', borderWidth: '3px', color: 'black'}, minWidth: 200, borderColor: 'black', backgroundColor: "black" , color: "#dbf3dd", borderWidth: '3px'}]} onClick={handelAddToCart} >ADD TO BAG</Button>
                        </div>
                        <div className="add__wish">
                            <IconButton disabled={props.item.atp < 0} variant="outlined" size="large" sx={[{'&:hover': { backgroundColor: '#dbf3dd', borderColor: '#dbf3dd', borderWidth: '3px', color: 'black'}, borderColor: 'black', backgroundColor: "black" , color: "#dbf3dd", borderWidth: '3px'}]} onClick={handelAddToWish}>
                                <FavoriteBorderIcon sx={{width: '22px', height: '22px'}}/>
                            </IconButton>
                        </div>
                    </div>
                </div>  
                </form>
                
            </div>
        </div>
     );
}
 
export default Detail;