import Carousel from 'react-bootstrap/Carousel';
import './ItemCarousel.css'
import soldoutImage from '../../../asset/img/sold-out.png'

const ProductCarousel = (props) => {
    return (
      <div className="product__carousel__container">
        <div className="product__carousel">
          <Carousel variant="dark" interval={4000}>
            <Carousel.Item>
            <div className="carousel__image__container">
                <img className="carousel__image" src={`${props.item.link}`} alt="item"/>
                {/* <img className="carousel__image" src={``} alt="item"/> */}
            </div>
            </Carousel.Item>  
            <Carousel.Item>
            <div className="carousel__image__container">
                <img className="carousel__image" src={`${props.item.link}`} alt="item"/>
                {/* <img className="carousel__image" src={``} alt="item"/> */}
              </div>
            </Carousel.Item>   
            {/* <Carousel.Item>
            <div className="carousel__image__container">
                <img className="carousel__image" src={``} alt="item"/>
              </div>
            </Carousel.Item> */}
          </Carousel>
        </div>
      </div>
     );
}
 
export default ProductCarousel;