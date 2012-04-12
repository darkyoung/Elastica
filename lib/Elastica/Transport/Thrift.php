<?php
// Setup the path to the thrift library folder
$GLOBALS['THRIFT_ROOT'] = ROOT_DIR . '/thrift';

// Load up all the thrift stuff
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';

// Load the package that used for elasticsearch
require_once $GLOBALS['THRIFT_ROOT'].'/packages/elasticsearch/Rest.php';

/**
 * Elastica Thrfit Transport object
 *
 * @category Xodoa
 * @package Elastica
 * @author Wu Yang <darkyoung@gmail.com>
 */
class Elastica_Transport_Thrift extends Elastica_Transport_Abstract {

	/**
	 * Makes calls to the elasticsearch server
	 *
	 * @param array $params host and port
	 * @return Elastica_Response Response object
	 */
	public function exec(array $params) {

		$request = $this->getRequest();

		$data = $request->getData();

		$content = '';

		if (!empty($data)) {
			if (is_array($data)) {
				$content = json_encode($data);
			} else {
				$content = $data;
			}

			// Escaping of / not necessary. Causes problems in base64 encoding of files
			$content = str_replace('\/', '/', $content);
		}

        $method = strtoupper($request->getMethod());
        $method = $GLOBALS['E_Method'][$method];

        $RestRequest = new RestRequest(array(
            'method' => $method,
            'uri' => $request->getPath(),
            'body' => $content
        ));

        //config server
        $socket = new TSocket($params['host'], $params['port']);
        $socket->setRecvTimeout($request->getConfig('timeout') * 1000);

        $transport = new TFramedTransport($socket);
        $protocol = new TBinaryProtocol($transport);
        $client = new RestClient($protocol);

        //open transport
        if (!$transport->isOpen()) {
            $transport->open();
        }

        $RestResponse = $client->execute($RestRequest);

        $response = new Elastica_Response($RestResponse->body);

		if ($response->hasError()) {
			throw new Elastica_Exception_Response($response);
		}

		return $response;
    }
}