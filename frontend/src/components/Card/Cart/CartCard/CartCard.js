import Box from '@mui/material/Box';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select from '@mui/material/Select';
import { useContext, useState } from 'react';
import HighlightOffIcon from '@mui/icons-material/HighlightOff';
import './CartCard.css';
import { CartItemsContext } from '../../../../Context/CartItemsContext';
import { IconButton } from '@mui/material';
import AddCircleIcon from '@mui/icons-material/AddCircle';
import RemoveCircleIcon from '@mui/icons-material/RemoveCircle';

const CartCard = (props) => {
    let cartItems  = useContext(CartItemsContext)

    const handelQuantityIncrement = (event) => {
        cartItems.quantity(props.item.id, 'INC');
    };

    const handelQuantityDecrement = (event) => {
        if(props.item.itemQuantity >1){
            cartItems.quantity(props.item.id, 'DEC');
        }
    };

    const handelRemoveItem = () => {
        cartItems.removeItem(props.item)
    }

    return (
        <div className='cart__item__card'>
            <div className="cart__item__detail">
                {/* <div className="cart__item__image">
                    <img src={``} alt="item" className="item__image"/>
                </div> */}
                <div className="cart__item__name">{props.item.title.slice(0, 30)}</div>
            </div>
            <div className="cart__item__quantity">
                <IconButton onClick={handelQuantityIncrement}>
                    <AddCircleIcon />
                </IconButton>
                <div type="text" name="quantity" className="quantity__input">{props.item.itemQuantity}</div>
                <IconButton onClick={handelQuantityDecrement}>
                    <RemoveCircleIcon fontSize='medium'/>
                </IconButton>
            </div>
            <div className="cart__item__price">₹{props.item.discountedPrice.toFixed(2)}</div>
            <div className="remove__item__icon">
                <IconButton>
                    <HighlightOffIcon onClick={handelRemoveItem}/>
                </IconButton>
            </div>
        </div>
     );
}
 
export default CartCard;