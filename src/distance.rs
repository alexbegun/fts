
fn push_to_window(hit_pos:u8, hit_word_index:u8, proximity_window: &mut Vec<(u8,u8)>)
{
    for i in 0..proximity_window.len() 
    {
        if proximity_window[i].1 == hit_word_index
        {
            proximity_window.remove(i);
            break;
        }
    }
    proximity_window.push((hit_pos,hit_word_index));
}

pub fn find_smallest_distance(vec:& Vec<u128>) ->(u8,bool)
{
    let mut all:u128 = 0;
    //Step 1. Or all inputs
    for p in vec
    {
        all|=p;
    }

    let mut i = 1 << 127;
    let mut hit_pos:u8 = 0;
    let mut proximity_window:Vec<(u8,u8)> = Vec::new();

    let mut smallest_distance:u8 = 128;
    let mut smallest_in_order:bool = false;
  
    while i>=1
    {
        //println!("{}",i);
        //if a hit then 
        if all & i > 0
        {

            let mut hit_word_index:u8 = 0;
            //figure out which word index hit
            for pi in 0..vec.len() 
            {
                if vec[pi] & i > 0
                {
                    hit_word_index = pi as u8;
                    break;
                }
            }

            push_to_window(hit_pos,hit_word_index, &mut proximity_window);

            //now check
            if proximity_window.len() == vec.len()
            {
                //println!("proximity window from:{} to: {}",proximity_window[0],proximity_window[vec.len()-1]);

                let from = proximity_window[0].0;
                let to = 127-proximity_window[vec.len()-1].0;

                let window_mask = u128::MAX<<from>>from>>to<<to;
                let mut prev_p:u128 = u128::MAX;
                let mut in_order = true;
                let mut is_hit = true;

                for p in vec
                {
                    //if even 1 word is not hit then get out.
                    if p & window_mask == 0
                    {
                        is_hit = false;
                        break;
                    }

                    if p & window_mask > prev_p
                    {
                        in_order = false;
                    }

                    prev_p = p & window_mask;
                }
               
                if is_hit
                {
                    //println!("is_hit:{}",is_hit);
                    let distance = proximity_window[vec.len()-1].0 - proximity_window[0].0;
                    if distance < smallest_distance //check if this distance is smaller than the previous smallest distance.
                    {
                        smallest_distance = distance;
                        smallest_in_order = in_order;
                        //println!("smallest proximity from:{} to: {} ordered:{}",proximity_window[0],proximity_window[vec.len()-1], in_order);
                    }
                    else if distance == smallest_distance && in_order //make sure that if the distances are the same and in_order then the smallest_in_order is set 
                    {
                        smallest_in_order = true;
                    }
                }

            }

        }
        i=i>>1;    
        hit_pos+=1;
    }

    (smallest_distance,smallest_in_order)
}