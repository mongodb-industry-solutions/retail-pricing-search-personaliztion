import './RelatedCard.css'
import { Link } from "react-router-dom"

const RelatedCard = (props) => {
    return ( 
        <div className="related__product__card__container">
            <div className="related__product__card__inner">
                <div className="related__product__image"> 
                    <img src= {`${props.item.link}`} alt="item" className="product__img"/> 
                </div>
                <div className="related__product__card__detail">
                    <div className="related__product__name">
                        <Link to={`/product/${props.item.id}`}>
                           {props.item.title.slice(0, 30)}
                        </Link>
                        
                    </div>
                    <div className="related__product__description">
                        <span>{props.item.mfg_brand_name}</span>
                    </div>
                    <div className="related__product__price">
                        <span>₹{props.item.discountedPrice.toFixed(2)}</span>
                    </div>
                </div>
            </div>
        </div>
     );
}
 
export default RelatedCard;