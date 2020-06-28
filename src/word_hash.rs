pub fn unhash_word(word_hash:u128) -> String
{
    let mut word = String::with_capacity(16);
    for i in 0..16
    {
        let c:u8 = (word_hash >> i*8) as u8;
        if c!=0
        {
            word.push(c as char);
        }
    }
    word = word.chars().rev().collect();
    word
}


pub fn hash_word_to_u128(word:&str) -> u128
{
    let mut r:u128 = 0;
    if word.len()<=16
    {
        for (i, c) in word.chars().enumerate() 
        {
            r = r | (c as u128) << (128 - (i + 1)*8);
        }
    }
    r
}

