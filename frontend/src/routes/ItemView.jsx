import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom'
import axios from 'axios'
import ReactLoading from 'react-loading';
import Item from '../components/Item/Item';
import { API_ENDPOINT } from '../config.js';

const ProductView = (props) => {
    const param = useParams()
    const [ item, setItem ] = useState()
    const [ loading, setLoading ] = useState(true)

    useEffect(() => {
        window.scrollTo(0, 0)
        axios.get(`${API_ENDPOINT}/retail/pid/` + param.id)
            .then(res => {
                setItem(res.data)
                setLoading(false)
            })
            .catch(err => console.log(err))

    }, [param.id])
    
    return (
            <div className="d-flex min-vh-100 w-100 justify-content-center align-items-center m-auto">
                {loading && <ReactLoading type="balls" color='#dbf3dd' height={100} width={100} className='m-auto'/>}
                {item && <Item item={item}/>}
            </div>
     );
}
 
export default ProductView;