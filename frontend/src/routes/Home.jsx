import { Fragment, useEffect, useState } from "react";
import axios from "axios";
import Landing from "../components/Landing/Landing";
import FeaturedItems from "../components/Featured/Items/FetauredItems";
import { TabTitle } from "../utils/General";
import { API_ENDPOINT } from '../config.js';


const Home = () => {
    const [ featuredItems, setFeaturedItems ] = useState()
    TabTitle("Home - Leafy");

    useEffect(() => {
        axios.get(`${API_ENDPOINT}/retail/featured`, {
            headers: {
              'Access-Control-Allow-Origin': '*'
            }
          })
            .then(res => setFeaturedItems(res.data))
            .catch(err => console.log(err))

        window.scrollTo(0, 0)
    }, [])

    return ( 
        <Fragment>
            <Landing />
            <FeaturedItems items={featuredItems}/>
        </Fragment>
    );
}
 
export default Home;