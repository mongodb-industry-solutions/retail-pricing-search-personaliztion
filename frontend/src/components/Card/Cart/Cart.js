import { Fragment, useContext, useState } from 'react';
import { CartItemsContext } from '../../../Context/CartItemsContext';
import Box from '@mui/material/Box';
import Modal from '@mui/material/Modal';
import ShoppingCartIcon from '@mui/icons-material/ShoppingCart';
import Badge from '@mui/material/Badge';
import CartCard from './CartCard/CartCard';
import './Cart.css'
import Button from '@mui/material/Button';
import axios from 'axios';
import { API_ENDPOINT } from '../../../config.js';



const style = {
  position: 'absolute',
  top: '50%',
  left: '50%',
  transform: 'translate(-50%, -50%)',
  minWidth: '350px',
  width: '45%',
  height: '400px',
  bgcolor: 'background.paper',
  border: '5px solid #dbf3dd',
  borderRadius: '15px',
  boxShadow: 24,
  p: 4,
};

const Cart = () => {
    const [open, setOpen] = useState(false);
    const handleOpen = () => setOpen(true);
    const handleClose = () => setOpen(false);
    const cartItems = useContext(CartItemsContext);

    const [ openCheckoutModal, setOpenCheckoutModal] = useState(false);
    const handleCheckoutOpen = () => {
        pushToCollection('purchase',cartItems.items)
        setOpenCheckoutModal(true);
    }
    const handleCheckoutClose = () => {
        setOpen(false);
        setOpenCheckoutModal(false);
    } 

    const pushToCollection = (eventType, cartItems) => {
        const currentDate = new Date();
        const formattedDate = currentDate.toISOString().slice(0, 19).replace('T', ' ');

        const messages = [];
        cartItems.forEach((item) => {
            const message = {
                event_time: `${formattedDate} UTC`,
                event_type: eventType,
                product_id: item.id,
                price: item.discountedPrice,
                user_session: generateRandomId(),
                count: item.itemQuantity
            };
            messages.push(message);
        });
      
        const data = {
          topic: 'clogs',
          message: messages
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

    

    const handleCheckout = async () => {
        if(cartItems.totalAmount > 0){
            const config = {
                reason: 'checkout',
                amount: cartItems.totalAmount
            }
        handleCheckoutOpen()
        //TODO: Capture checkout event
        }
        else {
            return
        }
    }

    return (
        <Fragment>
                <Badge badgeContent={cartItems.items.length} color="error">
                    <ShoppingCartIcon color="black" onClick={handleOpen} sx={{ width: '35px'}}/>
                </Badge>
                <Modal
                    open={open}
                    onClose={handleClose}
                >
                    <Box sx={style}>
                    <div className="cart__header">
                        <h2>Your Cart</h2>
                    </div>
                    <div className="cart__items__container">
                        <div className="cartItems">
                            {cartItems.items.length===0? 
                                <div className="cart__empty"> Empty cart!</div> : 
                                <div className="shop__cart__items">
                                    {cartItems.items.map((item) => <CartCard key={item.id} item={item}/>)}
                                </div>
                            }
                            {cartItems.items.length > 0 &&
                                <div className="options">
                                    <div className="total__amount">
                                        <div className="total__amount__label">Total Amount:</div>
                                        <div className="total__amount__value">₹{cartItems.totalAmount.toFixed(2)}</div>
                                    </div>
                                    <div className="checkout">
                                        <Button variant="outlined" onClick={handleCheckout}>Checkout</Button>
                                    </div>
                                </div>
                            }
                            </div>
                        </div>
                    </Box>
                </Modal>
                <Modal
                open={openCheckoutModal}
                onClose={handleCheckoutClose}
            >
                    <Box sx={style}>
                    <div className="d-flex w-100 h-100 justify-content-center align-items-center">
                        <h2>Your checkout was successful</h2>
                    </div>
                    </Box>
                </Modal>
        </Fragment>
     );
}
 
export default Cart;