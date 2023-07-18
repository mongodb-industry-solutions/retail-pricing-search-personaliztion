import SearchIcon from '@mui/icons-material/Search';
import { useNavigate } from 'react-router-dom';
import { useState } from 'react';
import './Form.css'
import { useContext } from 'react';
import { SearchContext } from '../../../Context/SearchContext';
import { API_ENDPOINT } from '../../../config.js';

const Form = () => {
  const [searchInput, setSearchInput] = useState('')
  const [suggestions, setSuggestions] = useState([])
  const searchContext = useContext(SearchContext)
  const navigate = useNavigate()

  const handleChange = (e) => {
    const inputValue = e.target.value
    setSearchInput(inputValue)

    // Fetch autocomplete suggestions based on input value
    fetch(`${API_ENDPOINT}/retail/autocomplete?query=${inputValue}`)
        .then(response => response.json())
        .then(data => {
            // Extract the suggestions from the array of objects
            const newSuggestions = data.map(item => item.query);
            setSuggestions(newSuggestions);
        })
      .catch(error => console.log(error))
  }

  const handleFormSubmit = (e) => {
    e.preventDefault();
    searchContext.setSearchQuery(searchInput);
    const queryParams = new URLSearchParams();
    queryParams.set('query', searchInput);
    navigate(`/search?${queryParams.toString()}`);
    setSearchInput('');
    setSuggestions([]);
  };
  

  const handleSuggestionClick = (suggestion) => {
    setSearchInput(suggestion)
    setSuggestions([])
  }

  return (
    <form className="search__form" onSubmit={handleFormSubmit}>
      
        <input type="text" placeholder='Search for products' className="search__form__input" value={searchInput} onChange={handleChange} required />
        {suggestions.length > 0 && (
          <div className="autocomplete__dropdown">
            {suggestions.map((suggestion, index) => (
              <div key={index} onClick={() => handleSuggestionClick(suggestion)}>
                {suggestion}
              </div>
            ))}
          </div>
        )}
     
      <button className="search__form__button" type='submit'>
        <SearchIcon fontSize='medium' />
      </button>
    </form>
  );
}

export default Form;
