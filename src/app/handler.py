from web3 import Web3
from web3 import AsyncWeb3
from web3.eth import AsyncEth
from web3._utils.events import get_event_data
from config import settings
from typing import List, Tuple
import asyncio
from collections import defaultdict
import requests
from datetime import datetime

class AsyncTokenAnalyzer:
    def __init__(self):
        self.w3 = AsyncWeb3(
            AsyncWeb3.AsyncHTTPProvider(settings.POLYGON_RPC),
            modules={'eth': (AsyncEth,)}
        )
        self.token_contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(settings.TOKEN_ADDRESS),
            abi=settings.ERC20_ABI
        )
        self.decimals = None
        self.symbol = None
    
    async def initialize(self):
        """Инициализация параметров токена"""
        self.decimals = await self.token_contract.functions.decimals().call()
        self.symbol = await self.token_contract.functions.symbol().call()
    
    async def get_balance(self, address: str) -> float:
        """Уровень A: Асинхронное получение баланса"""
        balance = await self.token_contract.functions.balanceOf(
            Web3.to_checksum_address(address)
        ).call()
        return self._to_decimal(balance)
    
    async def get_balance_batch(self, addresses: List[str]) -> List[float]:
        """Уровень B: Параллельное получение балансов"""
        tasks = [self.get_balance(addr) for addr in addresses]
        return await asyncio.gather(*tasks)
    
    async def get_top_holders(self, n: int = 10) -> List[Tuple[str, float]]:
        """Уровень C: Получение топ держателей"""
        if n <= 0:
            return []
        
        try:
            # 1. Получаем события с чанкованием
            transfer_events = await self._get_all_transfer_events()
            
            if not transfer_events:
                print("No transfer events found")
                return []
            
            # 2. Считаем балансы
            balances = defaultdict(int)
            for event in transfer_events:
                args = event['args']
                from_addr = args.get('from')
                to_addr = args.get('to')
                value = args.get('value', 0)
                
                if from_addr and from_addr != '0x0000000000000000000000000000000000000000':
                    balances[from_addr] -= value
                if to_addr and to_addr != '0x0000000000000000000000000000000000000000':
                    balances[to_addr] += value
            
            # 3. Фильтруем и конвертируем
            non_zero = [
                (addr, bal / (10 ** self.decimals))
                for addr, bal in balances.items() 
                if bal > 0
            ]
            
            # 4. Сортируем и возвращаем топ N
            return sorted(non_zero, key=lambda x: x[1], reverse=True)[:n]
        
        except Exception as e:
            print(f"Error in get_top_holders: {str(e)}")
            raise requests.HTTPError(
                status_code=500,
                detail="Failed to get top holders"
            )
        
    async def get_top_with_transactions(self, n: int = 10) -> List[Tuple[str, float, str]]:
        """
        Уровень D: Получает топ N адресов с балансами и датами последних транзакций
        (Не доделано)

        Параметры:
            n: количество возвращаемых записей
            
        Возвращает:
            Список кортежей (адрес, баланс, дата_последней_транзакции)
        """
        # 1. Получаем топ держателей
        top_holders = await self.get_top_holders(n)
        if not top_holders:
            return []
        
        # 2. Получаем даты последних транзакций для каждого адреса
        results = []
        for address, balance in top_holders:
            last_tx_date = await self._get_last_transaction_date(address)
            results.append((address, balance, last_tx_date))
        
        return results
    
    async def _get_last_transaction_date(self, address: str) -> str:
        """
        Получает дату последней транзакции для адреса
        (не хватило времени на чанкование, для более быстрого отбора)
        (На тестирование тоже времени не хватило)
        
        Параметры:
            address: адрес кошелька
            
        Возвращает:
            Строку с датой в формате "YYYY-MM-DD HH:MM:SS"
            или "No transactions", если транзакций не найдено
        """
        try:
            # Кодируем адрес для topics
            address_topic = self.w3.to_hex(
                self.w3.codec.encode(['address'], [address])  # Берем только последние 20 байт
            )
            
            # Получаем последнюю входящую транзакцию
            incoming_tx = await self.w3.eth.get_logs({
                "fromBlock": 0,
                "toBlock": "latest",
                "address": self.token_contract.address,
                "topics": [
                    "0x" + self.w3.keccak(text="Transfer(address,address,uint256)").hex(),
                    None,  # Любой отправитель
                    address_topic  # Конкретный получатель
                ],
                "limit": 1
            })
            
            # Получаем последнюю исходящую транзакцию
            outgoing_tx = await self.w3.eth.get_logs({
                "fromBlock": 0,
                "toBlock": "latest",
                "address": self.token_contract.address,
                "topics": [
                    "0x" + self.w3.keccak(text="Transfer(address,address,uint256)").hex(),
                    address_topic,  # Конкретный отправитель
                    None  # Любой получатель
                ],
                "limit": 1
            })
            
            # Выбираем самую свежую транзакцию
            last_tx = None
            if incoming_tx and outgoing_tx:
                last_tx = max(incoming_tx[0], outgoing_tx[0], key=lambda x: x['blockNumber'])
            elif incoming_tx:
                last_tx = incoming_tx[0]
            elif outgoing_tx:
                last_tx = outgoing_tx[0]
            
            if last_tx:
                block = await self.w3.eth.get_block(last_tx['blockNumber'])
                timestamp = block['timestamp']
                return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
            
            return "No transactions"
            
        except Exception as e:
            print(f"Error getting last transaction for {address}: {str(e)}")
            return "Error"
            
    async def get_token_info(self) -> dict:
        """Уровень E: Получение информации о токене"""
        name, total_supply = await asyncio.gather(
            self.token_contract.functions.name().call(),
            self.token_contract.functions.totalSupply().call()
        )
        
        return {
            "symbol": self.symbol,
            "name": name,
            "totalSupply": self._to_decimal(total_supply),
            "decimals": self.decimals,
            "address": settings.TOKEN_ADDRESS
        }
        
    async def _get_all_transfer_events(self, max_retries=3, chunk_size=1000000):
        """Получение событий Transfer с чанкованием и повторными попытками"""
        events = []
        current_block = await self.w3.eth.block_number
        from_block = 0
        
        while from_block <= current_block:
            to_block = min(from_block + chunk_size - 1, current_block)
            retries = 0
            
            while retries < max_retries:
                try:
                    # Получаем логи через eth_getLogs
                    logs = await asyncio.wait_for(
                        self.w3.eth.get_logs({
                            "fromBlock": from_block,
                            "toBlock": to_block,
                            "address": self.token_contract.address,
                            "topics": [
                                "0x" + self.w3.keccak(text="Transfer(address,address,uint256)").hex()
                            ]
                        }),
                        timeout=30  # Таймаут 30 секунд на запрос
                    )
                    
                    # Парсим логи в события
                    transfer_abi = self.token_contract.events.Transfer._get_event_abi()
                    for log in logs:
                        try:
                            event = get_event_data(self.w3.codec, transfer_abi, log)
                            events.append(event)
                        except Exception as e:
                            print(f"Error parsing event: {str(e)}")
                    
                    print(f"Processed blocks {from_block}-{to_block}, events: {len(events)}")
                    from_block = to_block + 1
                    break
                    
                except asyncio.TimeoutError:
                    retries += 1
                    print(f"Timeout, retry {retries}/{max_retries} for blocks {from_block}-{to_block}")
                    await asyncio.sleep(2 ** retries)  # Экспоненциальная задержка
                    
                except Exception as e:
                    print(f"Error getting logs for blocks {from_block}-{to_block}: {str(e)}")
                    retries = max_retries  # Пропускаем этот блок
                    break
        
        return events

    def _to_decimal(self, value: int) -> float:
            """Конвертация с учетом decimals"""
            return value / (10 ** self.decimals)

async def main():
    analyzer = AsyncTokenAnalyzer()
    await analyzer.initialize()
    
    # # Уровень A
    # balance = await analyzer.get_balance('0x51f1774249Fc2B0C2603542Ac6184Ae1d048351d')
    # print(f"Balance: {balance} {analyzer.symbol}")
    
    # # Уровень B
    # addresses = [
    #     "0x51f1774249Fc2B0C2603542Ac6184Ae1d048351d",
    #     "0x4830AF4aB9cd9E381602aE50f71AE481a7727f7C"
    # ]
    # balances = await analyzer.get_balance_batch(addresses)
    # print(f"Balances: {balances}")
    
    # # Уровень C
    # top_holders = await analyzer.get_top_holders(5)
    # print(f"Top holders: {top_holders}")

    # Уровень D
    top_holders_date = await analyzer.get_top_with_transactions(5)
    print(f"Top holders with date: {top_holders_date}") 
    
    # # Уровень E
    # token_info = await analyzer.get_token_info()
    # print(f"Token info: {token_info}")

if __name__ == "__main__":
    asyncio.run(main())