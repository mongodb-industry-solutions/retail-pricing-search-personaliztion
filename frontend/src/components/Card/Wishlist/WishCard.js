import { useContext } from 'react';
import HighlightOffIcon from '@mui/icons-material/HighlightOff';
import { IconButton } from '@mui/material';
import './WishCard.css'
import { Button } from '@mui/material';
import { WishItemsContext } from '../../../Context/WishItemsContext';

const WishCard = (props) => {

    const wishItems = useContext(WishItemsContext)

    const handelRemoveItem = () => {
        wishItems.removeItem(props.item)
    }

    const handelAddToCart = () => {
        wishItems.addToCart(props.item)
    };

    return ( 
        <div className="wishcard">
             <div className="wish__remove__item__icon">
                <IconButton>
                    <HighlightOffIcon onClick={handelRemoveItem}/>
                </IconButton>
            </div>
            {/* <div className="wish__item__image">
                <img src={`https://shema-ecommerce.herokuapp.com/${props.item.category}/${props.item.image[0].filename}`} alt="item" className="wish__image"/>
            </div> */}
            <div className="wish__item__name">{props.item.title.slice(0,25)}</div>
            <div className="wish__item__price">₹{props.item.discountedPrice.toFixed(2)}</div>
            <div className="add__to__cart">
                <Button variant='outlined' onClick={handelAddToCart} sx={[{'&:hover': { backgroundColor: '#dbf3dd', borderColor: '#dbf3dd', color: 'black'}, borderColor: 'black', backgroundColor: "black" , color: "#dbf3dd"}]}>Add to cart</Button>
            </div>
        </div>
     );
}
 
export default WishCard;