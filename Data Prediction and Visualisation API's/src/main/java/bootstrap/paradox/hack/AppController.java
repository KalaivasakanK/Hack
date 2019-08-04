package bootstrap.paradox.hack;

import java.io.IOException;
import java.util.Date;

import org.apache.http.client.ClientProtocolException;
import org.json.JSONException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
public class AppController {

	APIProcessorService apiService= new APIProcessorService();
	
    @GetMapping("/predict")
    @ResponseBody
    public Integer computeTime(@RequestParam(name="lat1") String lat1, @RequestParam(name="lat2") String lat2, @RequestParam(name="lon1") String lon1, @RequestParam(name="lon2") String lon2 ,@RequestParam(name="hrs") int hrs ) throws ClientProtocolException, IOException, JSONException 
    {
        return apiService.getTimeInterval(lat1, lon1, lat2, lon2, hrs);
    }
    
    @PostMapping("/visualise")
    @ResponseBody
    public String visualise(@RequestParam(name="hour") String hours, @RequestParam(name="fromDate") Date toDate ) {

        	return apiService.getVisualisationData(Integer.valueOf(hours), toDate);    
    
}
    }