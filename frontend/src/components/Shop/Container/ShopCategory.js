import ItemCard from "../../Card/ItemCard/ItemCard";
import { Button } from '@mui/material';
import './ShopCategory.css'

const ShopCategory = (props) => {
    return ( 
        <div className="shop__category__container">
            <div className="shop__category__header">
                <div className="shop__category__header__big">
                    <div className="shop__category__head">
                        <h2>Shop Online</h2>
                    </div> 
                    <div className="shop__category__header__line"></div>
                </div>
                </div>
                <div className="shop__category__card__container">
                    <div className="shop__category__product__card">
                        {props.items.map((data) => <ItemCard item={data} category={'vittal'}/>)}
                    </div>
                    <div className="show__more__action">
                            <Button variant='outlined' sx={[ {width: '200px', height: '50px', borderRadius: '20px' , fontWeight: '700', backgroundColor: '#dbf3dd', borderColor: '#dbf3dd', color: 'black' }, {'&:hover': { borderColor: '#dbf3dd', backgroundColor: "none" }}]}>Show more</Button>
                    </div>
            </div>
        </div>
     );
}
 
export default ShopCategory;