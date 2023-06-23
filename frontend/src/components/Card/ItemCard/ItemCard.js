import './ItemCard.css';
import { useContext, useState } from "react";
import { Link } from "react-router-dom";
import { CartItemsContext } from "../../../Context/CartItemsContext";
import { IconButton } from '@mui/material';
import AddShoppingCartIcon from '@mui/icons-material/AddShoppingCart';
import FavoriteBorderIcon from '@mui/icons-material/FavoriteBorder';
import { WishItemsContext } from '../../../Context/WishItemsContext';
import soldoutImage from '../../../asset/img/sold-out.png'
import { API_ENDPOINT } from '../../../config.js';
import axios from 'axios';

const ItemCard = (props) => {
    const [isHovered, setIsHovered] = useState(false)
    const  cartItemsContext  = useContext(CartItemsContext)
    const wishItemsContext = useContext(WishItemsContext)

    const handleAddToWishList = () => {
        wishItemsContext.addItem(props.item)
        pushToCollection('view',props.item.discountedPrice, props.item.id)
    }

    const handleAddToCart = () => {
        cartItemsContext.addItem(props.item, 1)
        pushToCollection('cart',props.item.discountedPrice, props.item.id)
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
        <div className="product__card__card">
          <div className="product__card">
            <div className="product__image" onMouseEnter={() => setIsHovered(true)} onMouseLeave={() => setIsHovered(false)}> 
              {props.item.atp > 0 ? (
                    <img src={`${props.item.link}`} alt="item" className="product__img"/> 
                ) : (
                <img src={soldoutImage} alt="item" className="product__img"/>
              )}
            </div>
            <div className="product__card__detail">
              <div className="product__name">
                <Link to={`/product/${props.item.id}`}>
                  {props.item.title.slice(0, 30)}
                </Link>
              </div>
              <div className="product__description">
                <span>{props.item.mfg_brand_name.slice(0, 25)}</span>
              </div>
              <div className="price_elasticity">
                  <span className={`product__price-label product__price-elasticity ${props.item.price_elasticity !== undefined && props.item.price_elasticity < 0 ? "red" : "green"}`}>
                    Price Elasticity: {props.item.price_elasticity !== undefined ? (props.item.price_elasticity * 100).toFixed(2) : 0}
                  </span>
              </div>
              <div className="product__price">
                <span>₹{props.item.discountedPrice.toFixed(2)}</span>
              </div>
              <div className="product__card__action">
                <IconButton onClick={handleAddToWishList} sx={{borderRadius: '20px', width: '40px', height: '40px'}}>
                  <FavoriteBorderIcon sx={{width: '22px', height: '22px', color: 'black'}}/>
                </IconButton>
                <IconButton onClick={handleAddToCart} sx={{borderRadius: '20px', width: '40px', height: '40px'}}>
                  <AddShoppingCartIcon sx={{width: '22px', height: '22px', color: 'black'}}/>
                </IconButton>
              </div>
            </div>
          </div>
        </div>
      );      
}
 
export default ItemCard;