import { useLocation, useParams, useSearchParams } from "react-router-dom";
import Search from "../components/Search";
import { useState, useEffect, useContext } from 'react';
import axios from 'axios'
import ReactLoading from 'react-loading';
import Category from '../components/Category/Category';
import { SearchContext } from '../Context/SearchContext';
import { API_ENDPOINT } from '../config.js';

const SearchView = () => {
    const param = useParams()
    const search = useContext(SearchContext)
    const [ searchParam, setSearchParam ] = useSearchParams()
    const [ items, setItems ] = useState()
    const [ loading , setLoading ] = useState(true) 

    const searchQuery = {
        query: search.searchQuery
    }
    src/routes/Search.jsx
    useEffect(() => {
        // TODO: Fix search param context
        // setSearchParam(searchQuery, { replace: true })
        const query = new URLSearchParams(window.location.search).get('query')
        var url = `${API_ENDPOINT}/retail/search?query=` + encodeURIComponent(query)
         // Check if vector search is enabled
         const isVectorSearch = new URLSearchParams(window.location.search).get('vecSearch')
         if (isVectorSearch) {
             url += '&vecSearch=True'
         }
        axios.get(url, {
            headers: {
              'Access-Control-Allow-Origin': '*'
            }
          })
            .then(res => {
                setItems(res.data)
                setLoading(false)
            })
            .catch(err => console.log(err))

        window.scrollTo(0, 0)
    }, [searchQuery.query])

    return ( 
        <div className='d-flex min-vh-100 w-100 justify-content-center align-items-center m-auto'>
            {loading && <ReactLoading type="balls" color='#dbf3dd' height={100} width={100} className='m-auto'/>}
            { ( !items || typeof items === 'undefined' || items.length == 0) && !loading &&<Search/>}
            { typeof items != 'undefined' && items.length >0 && !loading && <Category name="Search Results" items={items} category="men"/>}
          
        </div>
        
          
     );
}
 
export default SearchView;